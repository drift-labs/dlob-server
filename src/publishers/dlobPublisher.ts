import { program } from 'commander';

import { Connection, Commitment, PublicKey, Keypair } from '@solana/web3.js';

import {
	DriftClient,
	initialize,
	DriftEnv,
	UserMap,
	Wallet,
	BulkAccountLoader,
	SlotSource,
	DriftClientSubscriptionConfig,
	SlotSubscriber,
	isVariant,
	OracleInfo,
	PerpMarketConfig,
	SpotMarketConfig,
	PhoenixSubscriber,
	decodeName,
	WebSocketAccountSubscriberV2,
	ONE,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';

import { logger, setLogLevel } from '../utils/logger';
import {
	SubscriberLookup,
	getOpenbookSubscriber,
	parsePositiveIntArray,
	sleep,
} from '../utils/utils';
import {
	DLOBSubscriberIO,
	wsMarketInfo,
} from '../dlob-subscriber/DLOBSubscriberIO';
import {
	DLOBProvider,
	getDLOBProviderFromOrderSubscriber,
	getDLOBProviderFromUserMap,
} from '../dlobProvider';
import FEATURE_FLAGS from '../utils/featureFlags';
import express, { Response, Request } from 'express';
import { handleHealthCheck } from '../core/middleware';
import { setGlobalDispatcher, Agent } from 'undici';
import { Metrics } from '../core/metricsV2';
import { OrderSubscriberFiltered } from '../dlob-subscriber/OrderSubscriberFiltered';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();
const stateCommitment: Commitment = 'confirmed';
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
const metricsPort = process.env.METRICS_PORT
	? parseInt(process.env.METRICS_PORT)
	: 9464;

const REDIS_CLIENT = process.env.REDIS_CLIENT || 'DLOB';

// Set up express for health checks
const app = express();

// init metrics
const metricsV2 = new Metrics('dlob-publisher', undefined, metricsPort);
const healthStatusGauge = metricsV2.addGauge(
	'health_status',
	'Health check status'
);
const dlobSlotGauge = metricsV2.addGauge(
	'dlob_slot',
	'Last updated slot of DLOB'
);
const oracleSlotGauge = metricsV2.addGauge(
	'oracle_slot',
	'Last updated slot of oracle'
);
const marketSlotGauge = metricsV2.addGauge(
	'market_slot',
	'Last updated slot of market account'
);
const kinesisRecordsSentCounter = metricsV2.addCounter(
	'kinesis_records_sent',
	'Number of records sent to Kinesis'
);
metricsV2.finalizeObservables();

//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });
let driftClient: DriftClient;

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const useGrpc = process.env.USE_GRPC?.toLowerCase() === 'true';
const useWebsocket = process.env.USE_WEBSOCKET?.toLowerCase() === 'true';

const token = process.env.TOKEN;
const endpoint = process.env.ENDPOINT;
const grpcEndpoint = useGrpc
	? process.env.GRPC_ENDPOINT ?? endpoint + `/${token}`
	: '';

const wsEndpoint = process.env.WS_ENDPOINT;
const useOrderSubscriber =
	process.env.USE_ORDER_SUBSCRIBER?.toLowerCase() === 'true';

const ORDERBOOK_UPDATE_INTERVAL =
	parseInt(process.env.ORDERBOOK_UPDATE_INTERVAL) || 400;
const WS_FALLBACK_FETCH_INTERVAL = 60_000;

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

const enableOffloadQueue = process.env.ENABLE_OFFLOAD === 'true';
const ignoreList = process.env.IGNORE_LIST?.split(',') || [
	'5N1AcdftujhXWZdBaqfciaKtXn6uVBKjmgwf6aQxR1vW',
];

logger.info(`RPC endpoint:  ${endpoint}`);
logger.info(`WS endpoint:   ${wsEndpoint}`);
logger.info(`GRPC endpoint: ${grpcEndpoint}`);
logger.info(`GRPC Token:    ${token}`);
logger.info(
	`useOrderSubscriber: ${useOrderSubscriber}, useWebsocket: ${useWebsocket}, useGrpc: ${useGrpc}`
);
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
				const bulkAccountLoader = new BulkAccountLoader(
					driftClient.connection,
					stateCommitment,
					2_000
				);
				const phoenixSubscriber = new PhoenixSubscriber({
					connection: driftClient.connection,
					programId: new PublicKey(sdkConfig.PHOENIX),
					marketAddress: phoenixConfigAccount.phoenixMarket,
					accountSubscription: {
						type: 'polling',
						accountLoader: bulkAccountLoader,
					},
				});
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

		if (marketConfig.openbookMarket) {
			const openbookMarketAccount =
				await driftClient.getOpenbookV2FulfillmentConfig(
					marketConfig.openbookMarket
				);

			if (isVariant(openbookMarketAccount.status, 'enabled')) {
				logger.info(
					`Loading openbook subscriber for spot market ${market.marketIndex}`
				);
				const openbookSubscriber = getOpenbookSubscriber(
					driftClient,
					marketConfig,
					sdkConfig
				);
				await openbookSubscriber.subscribe();
				try {
					openbookSubscriber.getL2Asks();
					openbookSubscriber.getL2Bids();
					markets[market.marketIndex].openbook = openbookSubscriber;
				} catch (e) {
					logger.info(
						`Excluding openbook for ${market.marketIndex}, error: ${e}`
					);
				}
			}
		}

		markets[market.marketIndex].tickSize = market?.orderTickSize ?? ONE;
	}

	return markets;
};

const main = async () => {
	const wallet = new Wallet(new Keypair());
	const clearingHousePublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);

	const redisClient = new RedisClient({
		prefix: RedisClientPrefix[REDIS_CLIENT],
	});
	await redisClient.connect();

	const indicativeRedisClient = new RedisClient({});
	await indicativeRedisClient.connect();

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

	// NOTE: disable GRPC for general driftClient subscriptions until we can reliably subscribe
	// to multiple streams. Currently this causes the nodes to start killing connections.
	//
	// USE_GRPC=true will override websocket
	// if (useGrpc) {
	// 	accountSubscription = {
	// 		type: 'grpc',
	// 		resubTimeoutMs: 30_000,
	// 		grpcConfigs: {
	// 			endpoint,
	// 			token,
	// 			channelOptions: {
	// 				'grpc.keepalive_time_ms': 10_000,
	// 				'grpc.keepalive_timeout_ms': 1_000,
	// 				'grpc.keepalive_permit_without_calls': 1,
	// 			},
	// 		},
	// 	};

	// 	slotSubscriber = new SlotSubscriber(connection);
	// 	await slotSubscriber.subscribe();

	// 	slotSource = {
	// 		getSlot: () => slotSubscriber!.getSlot(),
	// 	};
	// }

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
			logResubMessages: true,
			perpMarketAccountSubscriber: WebSocketAccountSubscriberV2,
		};
		slotSubscriber = new SlotSubscriber(connection, {
			resubTimeoutMs: 10_000,
		});
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
		let subscriptionConfig: any = {
			type: 'polling',
			commitment: stateCommitment,
			frequency: ORDERBOOK_UPDATE_INTERVAL,
		};

		if (useWebsocket) {
			subscriptionConfig = {
				type: 'websocket',
				commitment: stateCommitment,
			};
		}

		// USE_GRPC=true will override websocket
		if (useGrpc) {
			if (!token) {
				throw new Error('TOKEN is required for grpc');
			}
			if (!grpcEndpoint) {
				throw new Error(
					'GRPC_ENDPOINT is required for grpc (or ENDPOINT and TOKEN)'
				);
			}
			if (useWebsocket) {
				logger.warn('USE_GRPC overriding USE_WEBSOCKET');
			}
			subscriptionConfig = {
				type: 'grpc',
				grpcConfigs: {
					endpoint: grpcEndpoint,
					token: token,
					commitmentLevel: stateCommitment,
					channelOptions: {
						'grpc.keepalive_time_ms': 10_000,
						'grpc.keepalive_timeout_ms': 1_000,
						'grpc.keepalive_permit_without_calls': 1,
					},
				},
				commitment: stateCommitment,
			};
		}

		const orderSubscriber = new OrderSubscriberFiltered({
			driftClient,
			subscriptionConfig,
			ignoreList,
		});

		dlobProvider = getDLOBProviderFromOrderSubscriber(orderSubscriber);

		slotSource = {
			getSlot: () => orderSubscriber.getSlot(),
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
		protectedMakerView: false,
	});
	await dlobSubscriber.subscribe();

	const dlobSubscriberIndicative = new DLOBSubscriberIO({
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
		protectedMakerView: false,
		indicativeQuotesRedisClient: indicativeRedisClient,
		enableOffloadQueue,
		offloadQueueCounter: kinesisRecordsSentCounter,
	});
	await dlobSubscriberIndicative.subscribe();

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

	setInterval(() => {
		const slot = slotSource.getSlot();
		perpMarketInfos.forEach((market) => {
			const oracleDataAndSlot = driftClient.getOracleDataForPerpMarket(
				market.marketIndex
			);
			const marketAccount =
				driftClient.accountSubscriber.getMarketAccountAndSlot(
					market.marketIndex
				);
			dlobSlotGauge.setLatestValue(slot, {
				marketIndex: market.marketIndex,
				marketType: 'perp',
				marketName: market.marketName,
				redisClient: REDIS_CLIENT,
				redisPrefix: RedisClientPrefix[REDIS_CLIENT],
			});
			oracleSlotGauge.setLatestValue(oracleDataAndSlot.slot.toNumber(), {
				marketIndex: market.marketIndex,
				marketType: 'perp',
				marketName: market.marketName,
				redisClient: REDIS_CLIENT,
				redisPrefix: RedisClientPrefix[REDIS_CLIENT],
			});
			marketSlotGauge.setLatestValue(marketAccount.slot, {
				marketIndex: market.marketIndex,
				marketType: 'perp',
				marketName: market.marketName,
				redisClient: REDIS_CLIENT,
				redisPrefix: RedisClientPrefix[REDIS_CLIENT],
			});
		});
		spotMarketInfos.forEach((market) => {
			const oracleDataAndSlot = driftClient.getOracleDataForSpotMarket(
				market.marketIndex
			);
			const marketAccount =
				driftClient.accountSubscriber.getSpotMarketAccountAndSlot(
					market.marketIndex
				);
			dlobSlotGauge.setLatestValue(slot, {
				marketIndex: market.marketIndex,
				marketType: 'spot',
				marketName: market.marketName,
				redisClient: REDIS_CLIENT,
				redisPrefix: RedisClientPrefix[REDIS_CLIENT],
			});
			oracleSlotGauge.setLatestValue(oracleDataAndSlot.slot.toNumber(), {
				marketIndex: market.marketIndex,
				marketType: 'spot',
				marketName: market.marketName,
				redisClient: REDIS_CLIENT,
				redisPrefix: RedisClientPrefix[REDIS_CLIENT],
			});
			marketSlotGauge.setLatestValue(marketAccount.slot, {
				marketIndex: market.marketIndex,
				marketType: 'spot',
				marketName: market.marketName,
				redisClient: REDIS_CLIENT,
				redisPrefix: RedisClientPrefix[REDIS_CLIENT],
			});
		});
	}, 10_000);

	const handleStartup = async (_req, res, _next) => {
		if (driftClient.isSubscribed && dlobProvider.size() > 0) {
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(500);
			res.end('Not ready');
		}
	};

	const handleDebug = async (req: Request, res: Response) => {
		const slot = slotSource.getSlot();
		const slotInfos = [];
		for (const market of driftClient.getPerpMarketAccounts()) {
			const oracleDataAndSlot = driftClient.getOracleDataForPerpMarket(
				market.marketIndex
			);
			const marketSlot = market.amm.lastUpdateSlot.toNumber();
			const oracleSlot = oracleDataAndSlot.slot.toNumber();
			slotInfos.push({
				marketName: decodeName(market.name),
				slot,
				marketSlot,
				oracleSlot,
				marketSlotDiff: marketSlot - slot,
				oracleSlotDiff: oracleSlot - slot,
			});
		}

		for (const market of driftClient.getSpotMarketAccounts()) {
			const oracleDataAndSlot = driftClient.getOracleDataForSpotMarket(
				market.marketIndex
			);
			const oracleSlot = oracleDataAndSlot.slot.toNumber();
			slotInfos.push({
				marketName: decodeName(market.name),
				slot,
				oracleSlot,
				oracleSlotDiff: oracleSlot - slot,
			});
		}

		res.json(slotInfos);
	};
	app.get('/debug', handleDebug);
	app.get('/health', handleHealthCheck(slotSource, healthStatusGauge));
	app.get('/startup', handleStartup);
	app.get('/', handleHealthCheck(slotSource, healthStatusGauge));
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
