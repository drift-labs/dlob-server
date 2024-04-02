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
	OracleInfo,
	PerpMarketConfig,
	SpotMarketConfig,
} from '@drift-labs/sdk';

import { logger, setLogLevel } from '../utils/logger';
import {
	SubscriberLookup,
	getPhoenixSubscriber,
	getSerumSubscriber,
	parsePositiveIntArray,
	sleep,
} from '../utils/utils';
import {
	DLOBSubscriberIO,
	wsMarketInfo,
} from '../dlob-subscriber/DLOBSubscriberIO';
import { RedisClient } from '../utils/redisClient';
import {
	DLOBProvider,
	getDLOBProviderFromGrpcOrderSubscriber,
	getDLOBProviderFromOrderSubscriber,
	getDLOBProviderFromUserMap,
} from '../dlobProvider';
import FEATURE_FLAGS from '../utils/featureFlags';
import { GeyserOrderSubscriber } from '../grpc/OrderSubscriberGRPC';
import express from 'express';
import { handleHealthCheck } from '../core/metrics';

require('dotenv').config();
const stateCommitment: Commitment = 'confirmed';
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

// Set up express for health checks
const app = express();

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

const KILLSWITCH_SLOT_DIFF_THRESHOLD =
	parseInt(process.env.KILLSWITCH_SLOT_DIFF_THRESHOLD) || 200;

// comma separated list of perp market indexes to load: i.e. 0,1,2,3
const PERP_MARKETS_TO_LOAD =
	process.env.PERP_MARKETS_TO_LOAD !== undefined
		? parsePositiveIntArray(process.env.PERP_MARKETS_TO_LOAD)
		: undefined;

// comma separated list of spot market indexes to load: i.e. 0,1,2,3
const SPOT_MARKETS_TO_LOAD =
	process.env.SPOT_MARKETS_TO_LOAD !== undefined
		? parsePositiveIntArray(process.env.SPOT_MARKETS_TO_LOAD)
		: undefined;

logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

let MARKET_SUBSCRIBERS: SubscriberLookup = {};

const getMarketsAndOraclesToLoad = (
	sdkConfig: any
): {
	perpMarketInfos: wsMarketInfo[];
	spotMarketInfos: wsMarketInfo[];
	oracleInfos?: OracleInfo[];
} => {
	const oracleInfos: OracleInfo[] = [];
	const oraclesTracked = new Set();
	const perpMarketInfos: wsMarketInfo[] = [];
	const spotMarketInfos: wsMarketInfo[] = [];

	// only watch all markets if neither env vars are specified
	const noMarketsSpecified = !PERP_MARKETS_TO_LOAD && !SPOT_MARKETS_TO_LOAD;

	let perpIndexes = PERP_MARKETS_TO_LOAD;
	if (!perpIndexes) {
		if (noMarketsSpecified) {
			perpIndexes = sdkConfig.PERP_MARKETS.map((m) => m.marketIndex);
		} else {
			perpIndexes = [];
		}
	}
	let spotIndexes = SPOT_MARKETS_TO_LOAD;
	if (!spotIndexes) {
		if (noMarketsSpecified) {
			spotIndexes = sdkConfig.SPOT_MARKETS.map((m) => m.marketIndex);
		} else {
			spotIndexes = [];
		}
	}

	if (perpIndexes.length > 0) {
		for (const idx of perpIndexes) {
			const perpMarketConfig = sdkConfig.PERP_MARKETS[idx] as PerpMarketConfig;
			if (!perpMarketConfig) {
				throw new Error(`Perp market config for ${idx} not found`);
			}
			const oracleKey = perpMarketConfig.oracle.toBase58();
			if (!oraclesTracked.has(oracleKey)) {
				logger.info(`Tracking oracle ${oracleKey} for perp market ${idx}`);
				oracleInfos.push({
					publicKey: perpMarketConfig.oracle,
					source: perpMarketConfig.oracleSource,
				});
				oraclesTracked.add(oracleKey);
			}
			perpMarketInfos.push({
				marketIndex: perpMarketConfig.marketIndex,
				marketName: perpMarketConfig.symbol,
			});
		}
		logger.info(
			`DlobPublisher tracking perp markets: ${JSON.stringify(perpMarketInfos)}`
		);
	}

	if (spotIndexes.length > 0) {
		for (const idx of spotIndexes) {
			const spotMarketConfig = sdkConfig.SPOT_MARKETS[idx] as SpotMarketConfig;
			if (!spotMarketConfig) {
				throw new Error(`Spot market config for ${idx} not found`);
			}
			const oracleKey = spotMarketConfig.oracle.toBase58();
			if (!oraclesTracked.has(oracleKey)) {
				logger.info(`Tracking oracle ${oracleKey} for spot market ${idx}`);
				oracleInfos.push({
					publicKey: spotMarketConfig.oracle,
					source: spotMarketConfig.oracleSource,
				});
				oraclesTracked.add(oracleKey);
			}
			spotMarketInfos.push({
				marketIndex: spotMarketConfig.marketIndex,
				marketName: spotMarketConfig.symbol,
			});
		}
		logger.info(
			`DlobPublisher tracking spot markets: ${JSON.stringify(spotMarketInfos)}`
		);
	}

	return {
		perpMarketInfos,
		spotMarketInfos,
		oracleInfos,
	};
};

const initializeAllMarketSubscribers = async (driftClient: DriftClient) => {
	const markets: SubscriberLookup = {};

	for (const market of driftClient.getSpotMarketAccounts()) {
		markets[market.marketIndex] = {
			phoenix: undefined,
			serum: undefined,
		};
		const marketConfig = sdkConfig.SPOT_MARKETS[market.marketIndex];

		if (marketConfig.phoenixMarket) {
			const phoenixConfigAccount =
				await driftClient.getPhoenixV1FulfillmentConfig(
					marketConfig.phoenixMarket
				);
			if (isVariant(phoenixConfigAccount.status, 'enabled')) {
				logger.info(
					`Loading phoenix subscriber for spot market ${market.marketIndex}`
				);
				const phoenixSubscriber = getPhoenixSubscriber(
					driftClient,
					marketConfig,
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

		if (marketConfig.serumMarket) {
			const serumConfigAccount = await driftClient.getSerumV3FulfillmentConfig(
				marketConfig.serumMarket
			);
			if (isVariant(serumConfigAccount.status, 'enabled')) {
				logger.info(
					`Loading serum subscriber for spot market ${market.marketIndex}`
				);
				const serumSubscriber = getSerumSubscriber(
					driftClient,
					marketConfig,
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

	const { perpMarketInfos, spotMarketInfos, oracleInfos } =
		getMarketsAndOraclesToLoad(sdkConfig);
	driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription,
		env: driftEnv,
		perpMarketIndexes: perpMarketInfos.map((m) => m.marketIndex),
		spotMarketIndexes: spotMarketInfos.map((m) => m.marketIndex),
		oracleInfos,
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
		env: driftEnv,
		dlobSource: dlobProvider,
		slotSource,
		updateFrequency: ORDERBOOK_UPDATE_INTERVAL,
		redisClient,
		spotMarketSubscribers: MARKET_SUBSCRIBERS,
		perpMarketInfos,
		spotMarketInfos,
		killSwitchSlotDiffThreshold: KILLSWITCH_SLOT_DIFF_THRESHOLD,
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

	const handleStartup = async (_req, res, _next) => {
		if (driftClient.isSubscribed && dlobProvider.size() > 0) {
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(500);
			res.end('Not ready');
		}
	};

	app.get(
		'/health',
		handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, dlobProvider)
	);
	app.get('/startup', handleStartup);
	app.get('/', handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, dlobProvider));
	const server = app.listen(8080);

	// Default keepalive is 5s, since the AWS ALB timeout is 60 seconds, clients
	// sometimes get 502s.
	// https://shuheikagawa.com/blog/2019/04/25/keep-alive-timeout/
	// https://stackoverflow.com/a/68922692
	server.keepAliveTimeout = 61 * 1000;
	server.headersTimeout = 65 * 1000;

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
