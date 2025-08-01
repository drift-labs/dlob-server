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
const tobResubscribeCounter = metricsV2.addCounter(
	'tob_resubscribe',
	'Number of TOB resubscribes triggered'
);
const tobStuckGauge = metricsV2.addGauge(
	'tob_stuck_duration',
	'Duration TOB has been stuck for each market'
);
metricsV2.finalizeObservables();

//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });
let driftClient: DriftClient;

const opts = program.opts();
setLogLevel('debug');

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

// TOB monitoring configuration
const ENABLE_TOB_MONITORING =
	process.env.ENABLE_TOB_MONITORING?.toLowerCase() === 'true';
const TOB_CHECK_INTERVAL = parseInt(process.env.TOB_CHECK_INTERVAL) || 60_000; // 1 minute
const TOB_STUCK_THRESHOLD = parseInt(process.env.TOB_STUCK_THRESHOLD) || 60_000; // 1 minute without change
const TOB_MONITORING_ENABLED_PERP_MARKETS = process.env
	.TOB_MONITORING_ENABLED_PERP_MARKETS
	? parsePositiveIntArray(process.env.TOB_MONITORING_ENABLED_PERP_MARKETS)
	: [0, 1, 2]; // Default to SOL-PERP, BTC-PERP, ETH-PERP

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
logger.info(
	`TOB Monitoring: ${ENABLE_TOB_MONITORING ? 'enabled' : 'disabled'}`
);
if (ENABLE_TOB_MONITORING) {
	logger.info(`TOB Check Interval: ${TOB_CHECK_INTERVAL}ms`);
	logger.info(`TOB Stuck Threshold: ${TOB_STUCK_THRESHOLD}ms`);
	logger.info(
		`TOB Monitoring Markets: ${TOB_MONITORING_ENABLED_PERP_MARKETS.join(', ')}`
	);
}

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
	let orderSubscriber: OrderSubscriberFiltered | undefined;

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

		orderSubscriber = new OrderSubscriberFiltered({
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

	// TOB monitoring for configured perp markets to detect stuck orders
	// Check if this node has any TOB monitoring markets configured
	const tobMonitoringMarketsInThisNode =
		TOB_MONITORING_ENABLED_PERP_MARKETS.filter((marketIndex) =>
			perpMarketInfos.some((market) => market.marketIndex === marketIndex)
		);

	const shouldEnableTobMonitoring =
		ENABLE_TOB_MONITORING &&
		useOrderSubscriber &&
		tobMonitoringMarketsInThisNode.length > 0;

	// Track last TOB update times and values for each TOB monitoring market
	const lastTobUpdateTimes = new Map<number, number>();
	const lastTobValues = new Map<number, { bid: string; ask: string }>();

	// Initialize TOB tracking for TOB monitoring markets in this node
	tobMonitoringMarketsInThisNode.forEach((marketIndex) => {
		lastTobUpdateTimes.set(marketIndex, Date.now());
		lastTobValues.set(marketIndex, { bid: '', ask: '' });
	});

	// Log TOB monitoring status
	if (ENABLE_TOB_MONITORING) {
		logger.info(
			`TOB Monitoring Markets in this node: ${tobMonitoringMarketsInThisNode.join(
				', '
			)}`
		);
		logger.info(
			`TOB Monitoring active: ${shouldEnableTobMonitoring ? 'yes' : 'no'}`
		);
	}

	// TOB monitoring function
	const checkTobForStuckOrders = async () => {
		if (!shouldEnableTobMonitoring) {
			return; // Only monitor when using OrderSubscriber, TOB monitoring is enabled, and node has major markets
		}

		logger.debug('Starting TOB monitoring check');

		const currentTime = Date.now();

		for (const marketIndex of tobMonitoringMarketsInThisNode) {
			try {
				// Get current TOB from DLOB
				const slot = slotSource.getSlot();
				const dlob = await dlobProvider.getDLOB(slot);

				// Get oracle data for the market
				const oracleData = driftClient.getOracleDataForPerpMarket(marketIndex);

				// Get L2 orderbook to check TOB
				const l2OrderBook = dlob.getL2({
					marketIndex,
					marketType: { perp: {} },
					slot,
					oraclePriceData: oracleData,
					depth: 1, // Only need top level
				});

				const bestBid = l2OrderBook.bids[0]?.price;
				const bestAsk = l2OrderBook.asks[0]?.price;

				if (bestBid && bestAsk) {
					const currentTobValues = {
						bid: bestBid.toString(),
						ask: bestAsk.toString(),
					};
					const lastTobValue = lastTobValues.get(marketIndex);
					const lastUpdate = lastTobUpdateTimes.get(marketIndex);

					// Check if TOB has changed
					const tobChanged =
						!lastTobValue ||
						lastTobValue.bid !== currentTobValues.bid ||
						lastTobValue.ask !== currentTobValues.ask;

					if (tobChanged) {
						// TOB changed, update tracking
						lastTobValues.set(marketIndex, currentTobValues);
						lastTobUpdateTimes.set(marketIndex, currentTime);
						logger.debug(
							`TOB updated for market ${marketIndex}: bid=${currentTobValues.bid}, ask=${currentTobValues.ask}`
						);
					} else if (
						lastUpdate &&
						currentTime - lastUpdate > TOB_STUCK_THRESHOLD
					) {
						// TOB has been stuck for too long, trigger resubscribe
						const stuckDuration = (currentTime - lastUpdate) / 1000;
						logger.warn(
							`TOB stuck for market ${marketIndex} for ${stuckDuration}s, triggering resubscribe`
						);

						// Update metrics
						tobStuckGauge.setLatestValue(stuckDuration, {
							marketIndex: marketIndex.toString(),
							marketType: 'perp',
						});

						// Get the OrderSubscriber instance for resubscribe
						if (orderSubscriber) {
							try {
								// Resubscribe and fetch to clear stuck state
								await orderSubscriber.unsubscribe();
								await orderSubscriber.subscribe();
								await orderSubscriber.fetch();

								logger.info(
									`Successfully resubscribed OrderSubscriber for market ${marketIndex}`
								);

								// Update metrics
								tobResubscribeCounter.add(1, {
									marketIndex: marketIndex.toString(),
									marketType: 'perp',
									success: 'true',
								});

								// Reset the timer after successful resubscribe
								lastTobUpdateTimes.set(marketIndex, currentTime);
							} catch (error) {
								logger.error(
									`Failed to resubscribe OrderSubscriber for market ${marketIndex}:`,
									error
								);

								// Update metrics for failed resubscribe
								tobResubscribeCounter.add(1, {
									marketIndex: marketIndex.toString(),
									marketType: 'perp',
									success: 'false',
								});
							}
						} else {
							logger.error(
								`OrderSubscriber not available for market ${marketIndex}`
							);
						}
					}
				}
			} catch (error) {
				logger.error(`Error checking TOB for market ${marketIndex}:`, error);
			}
		}
	};

	// Start TOB monitoring
	setInterval(checkTobForStuckOrders, TOB_CHECK_INTERVAL);

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
