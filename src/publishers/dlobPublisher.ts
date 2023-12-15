import { program } from 'commander';

import { Connection, Commitment, PublicKey, Keypair } from '@solana/web3.js';

import {
	DriftClient,
	initialize,
	DriftEnv,
	UserMap,
	Wallet,
	BulkAccountLoader,
	OrderSubscriber,
	SlotSource,
	DriftClientSubscriptionConfig,
	SlotSubscriber,
	isVariant,
} from '@drift-labs/sdk';

import { logger, setLogLevel } from '../utils/logger';
import {
	SubscriberLookup,
	getPhoenixSubscriber,
	getSerumSubscriber,
	sleep,
} from '../utils/utils';
import { DLOBSubscriberIO } from '../dlob-subscriber/DLOBSubscriberIO';
import { RedisClient } from '../utils/redisClient';
import {
	DLOBProvider,
	getDLOBProviderFromGrpcOrderSubscriber,
	getDLOBProviderFromOrderSubscriber,
	getDLOBProviderFromUserMap,
} from '../dlobProvider';
import FEATURE_FLAGS from '../utils/featureFlags';
import { GeyserOrderSubscriber } from '../grpc/OrderSubscriberGRPC';

require('dotenv').config();
const stateCommitment: Commitment = 'processed';
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });
let driftClient: DriftClient;

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const token = process.env.TOKEN;
const endpoint = token
	? process.env.ENDPOINT + `/${token}`
	: process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
const useOrderSubscriber =
	process.env.USE_ORDER_SUBSCRIBER?.toLowerCase() === 'true';

const useGrpc = process.env.USE_GRPC?.toLowerCase() === 'true';
const useWebsocket = process.env.USE_WEBSOCKET?.toLowerCase() === 'true';

const ORDERBOOK_UPDATE_INTERVAL =
	parseInt(process.env.ORDERBOOK_UPDATE_INTERVAL) || 1000;
const WS_FALLBACK_FETCH_INTERVAL = 10_000;

logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

let MARKET_SUBSCRIBERS: SubscriberLookup = {};

const initializeAllMarketSubscribers = async (driftClient: DriftClient) => {
	const markets: SubscriberLookup = {};

	for (const market of sdkConfig.SPOT_MARKETS) {
		markets[market.marketIndex] = {
			phoenix: undefined,
			serum: undefined,
		};

		if (market.phoenixMarket) {
			const phoenixConfigAccount =
				await driftClient.getPhoenixV1FulfillmentConfig(market.phoenixMarket);
			if (isVariant(phoenixConfigAccount.status, 'enabled')) {
				const phoenixSubscriber = getPhoenixSubscriber(
					driftClient,
					market,
					sdkConfig
				);
				await phoenixSubscriber.subscribe();
				// Test get L2 to know if we should add
				try {
					phoenixSubscriber.getL2Asks();
					phoenixSubscriber.getL2Bids();
					markets[market.marketIndex].phoenix = phoenixSubscriber;
				} catch (e) {
					logger.info(
						`Excluding phoenix for ${market.marketIndex}, error: ${e}`
					);
				}
			}
		}

		if (market.serumMarket) {
			const serumConfigAccount = await driftClient.getSerumV3FulfillmentConfig(
				market.serumMarket
			);
			if (isVariant(serumConfigAccount.status, 'enabled')) {
				const serumSubscriber = getSerumSubscriber(
					driftClient,
					market,
					sdkConfig
				);
				await serumSubscriber.subscribe();
				try {
					serumSubscriber.getL2Asks();
					serumSubscriber.getL2Bids();
					markets[market.marketIndex].serum = serumSubscriber;
				} catch (e) {
					logger.info(
						`Excluding phoenix for ${market.marketIndex}, error: ${e}`
					);
				}
			}
		}
	}

	return markets;
};

const main = async () => {
	const wallet = new Wallet(new Keypair());
	const clearingHousePublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);

	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	// only set when polling
	let bulkAccountLoader: BulkAccountLoader | undefined;

	// only set when using websockets
	let slotSubscriber: SlotSubscriber | undefined;

	let accountSubscription: DriftClientSubscriptionConfig;
	let slotSource: SlotSource;

	if (!useWebsocket) {
		bulkAccountLoader = new BulkAccountLoader(
			connection,
			stateCommitment,
			ORDERBOOK_UPDATE_INTERVAL < 1000 ? 1000 : ORDERBOOK_UPDATE_INTERVAL
		);

		accountSubscription = {
			type: 'polling',
			accountLoader: bulkAccountLoader,
		};

		slotSource = {
			getSlot: () => bulkAccountLoader!.getSlot(),
		};
	} else {
		accountSubscription = {
			type: 'websocket',
			commitment: stateCommitment,
			resubTimeoutMs: 30_000,
		};
		slotSubscriber = new SlotSubscriber(connection);
		await slotSubscriber.subscribe();

		slotSource = {
			getSlot: () => slotSubscriber!.getSlot(),
		};
	}

	driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription,
		env: driftEnv,
	});

	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`DriftClient ProgramId: ${driftClient.program.programId.toBase58()}`
	);
	logger.info(`Wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(` . SOL balance: ${lamportsBalance / 10 ** 9}`);

	await driftClient.subscribe();
	driftClient.eventEmitter.on('error', (e) => {
		logger.info('clearing house error');
		logger.error(e);
	});

	logger.info(`Initializing all market subscribers...`);
	const initAllMarketSubscribersStart = Date.now();
	MARKET_SUBSCRIBERS = await initializeAllMarketSubscribers(driftClient);
	logger.info(
		`All market subscribers initialized in ${
			Date.now() - initAllMarketSubscribersStart
		} ms`
	);

	let dlobProvider: DLOBProvider;
	if (useOrderSubscriber) {
		let subscriptionConfig;
		if (useWebsocket) {
			subscriptionConfig = {
				type: 'websocket',
				commitment: stateCommitment,
			};
		} else {
			subscriptionConfig = {
				type: 'polling',
				commitment: stateCommitment,
				frequency: ORDERBOOK_UPDATE_INTERVAL,
			};
		}

		const orderSubscriber = new OrderSubscriber({
			driftClient,
			subscriptionConfig,
		});

		dlobProvider = getDLOBProviderFromOrderSubscriber(orderSubscriber);

		slotSource = {
			getSlot: () => orderSubscriber.getSlot(),
		};
	} else if (useGrpc) {
		const grpcOrderSubscriber = new GeyserOrderSubscriber(driftClient, {
			endpoint: endpoint,
			token: token,
		});

		dlobProvider = getDLOBProviderFromGrpcOrderSubscriber(grpcOrderSubscriber);

		slotSource = {
			getSlot: () => grpcOrderSubscriber.getSlot(),
		};
	} else {
		const userMap = new UserMap({
			driftClient,
			subscriptionConfig: {
				type: 'websocket',
				resubTimeoutMs: 30_000,
				commitment: stateCommitment,
			},
			skipInitialLoad: false,
			includeIdle: false,
		});

		dlobProvider = getDLOBProviderFromUserMap(userMap);
	}

	await dlobProvider.subscribe();

	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	await redisClient.connect();

	const dlobSubscriber = new DLOBSubscriberIO({
		driftClient,
		dlobSource: dlobProvider,
		slotSource,
		updateFrequency: ORDERBOOK_UPDATE_INTERVAL,
		redisClient,
		spotMarketSubscribers: MARKET_SUBSCRIBERS,
	});
	await dlobSubscriber.subscribe();
	if (useWebsocket && !FEATURE_FLAGS.DISABLE_GPA_REFRESH) {
		const recursiveFetch = (delay = WS_FALLBACK_FETCH_INTERVAL) => {
			setTimeout(() => {
				dlobProvider
					.fetch()
					.catch((e) => {
						logger.error('Failed to fetch GPA');
						console.log(e);
					})
					.finally(() => {
						// eslint-disable-next-line @typescript-eslint/no-unused-vars
						recursiveFetch();
					});
			}, delay);
		};
		recursiveFetch();
	}

	console.log('DLOBSubscriber Publishing Messages');
};

async function recursiveTryCatch(f: () => void) {
	try {
		await f();
	} catch (e) {
		console.error(e);
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => main());

export { sdkConfig, endpoint, wsEndpoint, driftEnv, commitHash, driftClient };
