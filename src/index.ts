import { program } from 'commander';
import compression from 'compression';
import cors from 'cors';
import express from 'express';
import rateLimit from 'express-rate-limit';
import morgan from 'morgan';

import { Commitment, Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	BN,
	BulkAccountLoader,
	DLOBNode,
	DLOBOrder,
	DLOBOrders,
	DLOBOrdersCoder,
	DLOBSubscriber,
	DriftClient,
	DriftClientSubscriptionConfig,
	DriftEnv,
	SlotSource,
	SlotSubscriber,
	UserMap,
	Wallet,
	getUserStatsAccountPublicKey,
	getVariant,
	groupL2,
	initialize,
	isVariant,
	OrderSubscriber,
} from '@drift-labs/sdk';

import { logger, setLogLevel } from './utils/logger';

import * as http from 'http';
import {
	gpaFetchDurationHistogram,
	handleHealthCheck,
	accountUpdatesCounter,
	cacheHitCounter,
	setLastReceivedWsMsgTs,
	runtimeSpecsGauge,
} from './core/metrics';
import { handleResponseTime } from './core/middleware';
import {
	SubscriberLookup,
	addOracletoResponse,
	errorHandler,
	getPhoenixSubscriber,
	getSerumSubscriber,
	l2WithBNToStrings,
	normalizeBatchQueryParams,
	sleep,
	validateDlobQuery,
} from './utils/utils';
import FEATURE_FLAGS from './utils/featureFlags';
import {
	DLOBProvider,
	getDLOBProviderFromOrderSubscriber,
	getDLOBProviderFromUserMap,
} from './dlobProvider';
import { RedisClient } from './utils/redisClient';

require('dotenv').config();

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'processed';
const serverPort = process.env.PORT || 6969;
export const ORDERBOOK_UPDATE_INTERVAL = 1000;
const WS_FALLBACK_FETCH_INTERVAL = ORDERBOOK_UPDATE_INTERVAL * 10;
const useWebsocket = process.env.USE_WEBSOCKET?.toLowerCase() === 'true';
const useOrderSubscriber =
	process.env.USE_ORDER_SUBSCRIBER?.toLowerCase() === 'true';
const rateLimitCallsPerSecond = process.env.RATE_LIMIT_CALLS_PER_SECOND
	? parseInt(process.env.RATE_LIMIT_CALLS_PER_SECOND)
	: 1;
const loadTestAllowed = process.env.ALLOW_LOAD_TEST?.toLowerCase() === 'true';
const useRedis = process.env.USE_REDIS?.toLowerCase() === 'true';

const logFormat =
	':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :req[x-forwarded-for]';
const logHttp = morgan(logFormat, {
	skip: (_req, res) => res.statusCode <= 500,
});

let driftClient: DriftClient;

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);
app.use(logHttp);
app.use(handleResponseTime);
app.use(
	rateLimit({
		windowMs: 1000, // 1 second
		max: rateLimitCallsPerSecond,
		standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
		legacyHeaders: false, // Disable the `X-RateLimit-*` headers
		skip: (req, _res) => {
			if (!loadTestAllowed) {
				return false;
			}

			return req.headers['user-agent'].includes('k6');
		},
	})
);

// strip off /dlob, if the request comes from exchange history server LB
app.use((req, _res, next) => {
	if (req.url.startsWith('/dlob')) {
		req.url = req.url.replace('/dlob', '');
		if (req.url === '') {
			req.url = '/';
		}
	}
	next();
});

// Metrics defined here
const bootTimeMs = Date.now();
runtimeSpecsGauge.addCallback((obs) => {
	obs.observe(bootTimeMs, {
		commit: commitHash,
		driftEnv,
		rpcEndpoint: endpoint,
		wsEndpoint: wsEndpoint,
	});
});

app.use(errorHandler);
const server = http.createServer(app);

// Default keepalive is 5s, since the AWS ALB timeout is 60 seconds, clients
// sometimes get 502s.
// https://shuheikagawa.com/blog/2019/04/25/keep-alive-timeout/
// https://stackoverflow.com/a/68922692
server.keepAliveTimeout = 61 * 1000;
server.headersTimeout = 65 * 1000;

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const endpoint = process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
logger.info(`RPC endpoint:       ${endpoint}`);
logger.info(`WS endpoint:        ${wsEndpoint}`);
logger.info(`useWebsocket:       ${useWebsocket}`);
logger.info(`useOrderSubscriber: ${useOrderSubscriber}`);
logger.info(`DriftEnv:           ${driftEnv}`);
logger.info(`Commit:             ${commitHash}`);

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
		wsEndpoint,
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
			ORDERBOOK_UPDATE_INTERVAL
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
				frequency: ORDERBOOK_UPDATE_INTERVAL,
				commitment: stateCommitment,
			};
		}

		let updatesReceivedTotal = 0;
		const orderSubscriber = new OrderSubscriber({
			driftClient,
			subscriptionConfig,
		});
		orderSubscriber.eventEmitter.on(
			'updateReceived',
			(_pubkey: PublicKey, _slot: number, _dataType: 'raw' | 'decoded') => {
				setLastReceivedWsMsgTs(Date.now());
				// eslint-disable-next-line @typescript-eslint/no-unused-vars
				updatesReceivedTotal++;
				accountUpdatesCounter.add(1);
			}
		);

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

	const dlobCoder = DLOBOrdersCoder.create();

	await driftClient.subscribe();
	driftClient.eventEmitter.on('error', (e) => {
		logger.info('clearing house error');
		logger.error(e);
	});

	logger.info(`Initializing DLOB Provider...`);
	const initDLOBProviderStart = Date.now();
	await dlobProvider.subscribe();
	logger.info(
		`dlob provider initialized in ${Date.now() - initDLOBProviderStart} ms`
	);
	logger.info(`dlob provider size ${dlobProvider.size()}`);

	logger.info(
		`GPA refresh?: ${useWebsocket && !FEATURE_FLAGS.DISABLE_GPA_REFRESH}`
	);
	if (useWebsocket && !FEATURE_FLAGS.DISABLE_GPA_REFRESH) {
		const recursiveFetch = (delay = WS_FALLBACK_FETCH_INTERVAL) => {
			setTimeout(() => {
				const startFetch = Date.now();
				dlobProvider
					.fetch()
					.then(() => {
						gpaFetchDurationHistogram.record(Date.now() - startFetch);
					})
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

	logger.info(`Initializing DLOBSubscriber...`);
	const initDlobSubscriberStart = Date.now();
	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: dlobProvider,
		slotSource,
		updateFrequency: ORDERBOOK_UPDATE_INTERVAL,
	});
	await dlobSubscriber.subscribe();
	logger.info(
		`DLOBSubscriber initialized in ${Date.now() - initDlobSubscriberStart} ms`
	);

	let redisClient: RedisClient;
	if (useRedis) {
		logger.info('Connecting to redis');
		redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
		await redisClient.connect();
	}

	logger.info(`Initializing all market subscribers...`);
	const initAllMarketSubscribersStart = Date.now();
	MARKET_SUBSCRIBERS = await initializeAllMarketSubscribers(driftClient);
	logger.info(
		`All market subscribers initialized in ${
			Date.now() - initAllMarketSubscribersStart
		} ms`
	);

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
		handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, slotSource)
	);
	app.get('/startup', handleStartup);
	app.get('/', handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, slotSource));

	if (FEATURE_FLAGS.ENABLE_ORDERS_ENDPOINTS) {
		app.get('/orders/json/raw', async (_req, res, next) => {
			try {
				// object with userAccount key and orders object serialized
				const orders: Array<any> = [];
				const oracles: Array<any> = [];
				const slot = slotSource.getSlot();

				for (const market of driftClient.getPerpMarketAccounts()) {
					const oracle = driftClient.getOracleDataForPerpMarket(
						market.marketIndex
					);
					oracles.push({
						marketIndex: market.marketIndex,
						...oracle,
					});
				}

				for (const {
					userAccount,
					publicKey,
				} of dlobProvider.getUserAccounts()) {
					for (const order of userAccount.orders) {
						if (isVariant(order.status, 'init')) {
							continue;
						}

						orders.push({
							user: publicKey.toBase58(),
							order: order,
						});
					}
				}

				// respond with orders
				res.writeHead(200);
				res.end(
					JSON.stringify({
						slot,
						oracles,
						orders,
					})
				);
			} catch (e) {
				next(e);
			}
		});

		app.get('/orders/json', async (_req, res, next) => {
			try {
				// object with userAccount key and orders object serialized
				const slot = slotSource.getSlot();
				const orders: Array<any> = [];
				const oracles: Array<any> = [];
				for (const market of driftClient.getPerpMarketAccounts()) {
					const oracle = driftClient.getOracleDataForPerpMarket(
						market.marketIndex
					);
					const oracleHuman = {
						marketIndex: market.marketIndex,
						price: oracle.price.toString(),
						slot: oracle.slot.toString(),
						confidence: oracle.confidence.toString(),
						hasSufficientNumberOfDataPoints:
							oracle.hasSufficientNumberOfDataPoints,
					};
					if (oracle.twap) {
						oracleHuman['twap'] = oracle.twap.toString();
					}
					if (oracle.twapConfidence) {
						oracleHuman['twapConfidence'] = oracle.twapConfidence.toString();
					}
					oracles.push(oracleHuman);
				}
				for (const {
					userAccount,
					publicKey,
				} of dlobProvider.getUserAccounts()) {
					for (const order of userAccount.orders) {
						if (isVariant(order.status, 'init')) {
							continue;
						}

						const orderHuman = {
							status: getVariant(order.status),
							orderType: getVariant(order.orderType),
							marketType: getVariant(order.marketType),
							slot: order.slot.toString(),
							orderId: order.orderId,
							userOrderId: order.userOrderId,
							marketIndex: order.marketIndex,
							price: order.price.toString(),
							baseAssetAmount: order.baseAssetAmount.toString(),
							baseAssetAmountFilled: order.baseAssetAmountFilled.toString(),
							quoteAssetAmountFilled: order.quoteAssetAmountFilled.toString(),
							direction: getVariant(order.direction),
							reduceOnly: order.reduceOnly,
							triggerPrice: order.triggerPrice.toString(),
							triggerCondition: getVariant(order.triggerCondition),
							existingPositionDirection: getVariant(
								order.existingPositionDirection
							),
							postOnly: order.postOnly,
							immediateOrCancel: order.immediateOrCancel,
							oraclePriceOffset: order.oraclePriceOffset,
							auctionDuration: order.auctionDuration,
							auctionStartPrice: order.auctionStartPrice.toString(),
							auctionEndPrice: order.auctionEndPrice.toString(),
							maxTs: order.maxTs.toString(),
						};
						if (order.quoteAssetAmountFilled) {
							orderHuman['quoteAssetAmount'] =
								order.quoteAssetAmountFilled.toString();
							orderHuman['quoteAssetAmountFilled'] =
								order.quoteAssetAmountFilled.toString();
						}

						orders.push({
							user: publicKey.toBase58(),
							order: orderHuman,
						});
					}
				}

				// respond with orders
				res.writeHead(200);
				res.end(
					JSON.stringify({
						slot,
						oracles,
						orders,
					})
				);
			} catch (err) {
				next(err);
			}
		});

		app.get('/orders/idl', async (_req, res, next) => {
			try {
				const dlobOrders: DLOBOrders = [];

				for (const {
					userAccount,
					publicKey,
				} of dlobProvider.getUserAccounts()) {
					for (const order of userAccount.orders) {
						if (isVariant(order.status, 'init')) {
							continue;
						}

						dlobOrders.push({
							user: publicKey,
							order,
						} as DLOBOrder);
					}
				}

				res.writeHead(200);
				res.end(dlobCoder.encode(dlobOrders));
			} catch (err) {
				next(err);
			}
		});

		app.get('/orders/idlWithSlot', async (req, res, next) => {
			try {
				const { marketName, marketIndex, marketType } = req.query;
				const { normedMarketType, normedMarketIndex, error } =
					validateDlobQuery(
						driftClient,
						driftEnv,
						marketType as string,
						marketIndex as string,
						marketName as string
					);
				const useFilter =
					marketName !== undefined ||
					marketIndex !== undefined ||
					marketType !== undefined;

				if (useFilter) {
					if (
						error ||
						normedMarketType === undefined ||
						normedMarketIndex === undefined
					) {
						res.status(400).send(error);
						return;
					}
				}

				const dlobOrders: DLOBOrders = [];

				for (const {
					userAccount,
					publicKey,
				} of dlobProvider.getUserAccounts()) {
					for (const order of userAccount.orders) {
						if (isVariant(order.status, 'init')) {
							continue;
						}

						if (useFilter) {
							if (
								getVariant(order.marketType) !== getVariant(normedMarketType) ||
								order.marketIndex !== normedMarketIndex
							) {
								continue;
							}
						}

						dlobOrders.push({
							user: publicKey,
							order,
						} as DLOBOrder);
					}
				}

				res.end(
					JSON.stringify({
						slot: slotSource.getSlot(),
						data: dlobCoder.encode(dlobOrders).toString('base64'),
					})
				);
			} catch (err) {
				next(err);
			}
		});
	}

	app.get('/topMakers', async (req, res, next) => {
		try {
			const {
				marketName,
				marketIndex,
				marketType,
				side, // bid or ask
				limit, // number of unique makers to return, if undefined will return all
				includeUserStats,
			} = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
				driftClient,
				driftEnv,
				marketType as string,
				marketIndex as string,
				marketName as string
			);
			if (error) {
				res.status(400).send(error);
				return;
			}

			if (side !== 'bid' && side !== 'ask') {
				res.status(400).send('Bad Request: side must be either bid or ask');
				return;
			}
			const normedSide = (side as string).toLowerCase();
			const oracle = driftClient.getOracleDataForPerpMarket(normedMarketIndex);

			let normedLimit = undefined;
			if (limit) {
				if (isNaN(parseInt(limit as string))) {
					res
						.status(400)
						.send('Bad Request: limit must be a number if supplied');
					return;
				}
				normedLimit = parseInt(limit as string);
			}

			const topMakers = new Set();
			let foundMakers = 0;
			const findMakers = async (sideGenerator: Generator<DLOBNode>) => {
				for (const side of sideGenerator) {
					if (limit && foundMakers >= normedLimit) {
						break;
					}
					if (side.userAccount) {
						const maker = side.userAccount.toBase58();
						if (topMakers.has(maker)) {
							continue;
						} else {
							if (`${includeUserStats}`.toLowerCase() === 'true') {
								const userAccount = dlobProvider.getUserAccount(
									side.userAccount
								);
								topMakers.add([
									userAccount,
									getUserStatsAccountPublicKey(
										driftClient.program.programId,
										userAccount.authority
									),
								]);
							} else {
								topMakers.add(side.userAccount.toBase58());
							}
							foundMakers++;
						}
					} else {
						continue;
					}
				}
			};

			if (normedSide === 'bid') {
				await findMakers(
					dlobSubscriber
						.getDLOB()
						.getRestingLimitBids(
							normedMarketIndex,
							slotSource.getSlot(),
							normedMarketType,
							oracle
						)
				);
			} else {
				await findMakers(
					dlobSubscriber
						.getDLOB()
						.getRestingLimitAsks(
							normedMarketIndex,
							slotSource.getSlot(),
							normedMarketType,
							oracle
						)
				);
			}

			res.writeHead(200);
			res.end(JSON.stringify([...topMakers]));
		} catch (err) {
			next(err);
		}
	});

	app.get('/l2', async (req, res, next) => {
		try {
			const {
				marketName,
				marketIndex,
				marketType,
				depth,
				numVammOrders,
				includeVamm,
				includePhoenix,
				includeSerum,
				grouping, // undefined or PRICE_PRECISION
				includeOracle,
			} = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
				driftClient,
				driftEnv,
				marketType as string,
				marketIndex as string,
				marketName as string
			);
			if (error) {
				res.status(400).send(error);
				return;
			}

			const isSpot = isVariant(normedMarketType, 'spot');

			let adjustedDepth = depth ?? '10';
			if (grouping !== undefined) {
				// If grouping is also supplied, we want the entire book depth.
				// we will apply depth after grouping
				adjustedDepth = '-1';
			}

			let l2Formatted: any;
			if (useRedis) {
				if (
					!isSpot &&
					`${includeVamm}`?.toLowerCase() === 'true' &&
					`${includeOracle}`?.toLowerCase() === 'true' &&
					!grouping
				) {
					let redisL2: string;
					if (parseInt(adjustedDepth as string) === 5) {
						redisL2 = await redisClient.client.get(
							`last_update_orderbook_perp_${normedMarketIndex}_depth_5`
						);
					} else if (parseInt(adjustedDepth as string) === 20) {
						redisL2 = await redisClient.client.get(
							`last_update_orderbook_perp_${normedMarketIndex}_depth_20`
						);
					} else if (parseInt(adjustedDepth as string) === 100) {
						redisL2 = await redisClient.client.get(
							`last_update_orderbook_perp_${normedMarketIndex}_depth_100`
						);
					}
					if (
						redisL2 &&
						slotSource.getSlot() - parseInt(JSON.parse(redisL2).slot) < 10
					)
						l2Formatted = redisL2;
				} else if (
					isSpot &&
					`${includeSerum}`?.toLowerCase() === 'true' &&
					`${includePhoenix}`?.toLowerCase() === 'true' &&
					`${includeOracle}`?.toLowerCase() === 'true' &&
					!grouping
				) {
					let redisL2: string;
					if (parseInt(adjustedDepth as string) === 5) {
						redisL2 = await redisClient.client.get(
							`last_update_orderbook_spot_${normedMarketIndex}_depth_5`
						);
					} else if (parseInt(adjustedDepth as string) === 20) {
						redisL2 = await redisClient.client.get(
							`last_update_orderbook_spot_${normedMarketIndex}_depth_20`
						);
					} else if (parseInt(adjustedDepth as string) === 100) {
						redisL2 = await redisClient.client.get(
							`last_update_orderbook_spot_${normedMarketIndex}_depth_100`
						);
					}
					if (
						redisL2 &&
						slotSource.getSlot() - parseInt(JSON.parse(redisL2).slot) < 10
					)
						l2Formatted = redisL2;
				}

				if (l2Formatted) {
					cacheHitCounter.add(1, {
						miss: false,
					});
					res.writeHead(200);
					res.end(l2Formatted);
					return;
				}
			}

			const l2 = dlobSubscriber.getL2({
				marketIndex: normedMarketIndex,
				marketType: normedMarketType,
				depth: parseInt(adjustedDepth as string),
				includeVamm: isSpot ? false : `${includeVamm}`.toLowerCase() === 'true',
				numVammOrders: parseInt((numVammOrders ?? '100') as string),
				fallbackL2Generators: isSpot
					? [
							`${includePhoenix}`.toLowerCase() === 'true' &&
								MARKET_SUBSCRIBERS[normedMarketIndex].phoenix,
							`${includeSerum}`.toLowerCase() === 'true' &&
								MARKET_SUBSCRIBERS[normedMarketIndex].serum,
					  ].filter((a) => !!a)
					: [],
			});

			if (grouping) {
				const finalDepth = depth ? parseInt(depth as string) : 10;
				if (isNaN(parseInt(grouping as string))) {
					res
						.status(400)
						.send('Bad Request: grouping must be a number if supplied');
					return;
				}
				const groupingBN = new BN(parseInt(grouping as string));
				const l2Formatted = l2WithBNToStrings(
					groupL2(l2, groupingBN, finalDepth)
				);
				if (`${includeOracle}`.toLowerCase() === 'true') {
					addOracletoResponse(
						l2Formatted,
						driftClient,
						normedMarketType,
						normedMarketIndex
					);
				}
			} else {
				// make the BNs into strings
				l2Formatted = l2WithBNToStrings(l2);
				if (`${includeOracle}`.toLowerCase() === 'true') {
					addOracletoResponse(
						l2Formatted,
						driftClient,
						normedMarketType,
						normedMarketIndex
					);
				}
			}
			cacheHitCounter.add(1, {
				miss: true,
			});
			res.writeHead(200);
			res.end(JSON.stringify(l2Formatted));
		} catch (err) {
			next(err);
		}
	});

	app.get('/batchL2', async (req, res, next) => {
		try {
			const {
				marketName,
				marketIndex,
				marketType,
				depth,
				includeVamm,
				includePhoenix,
				includeSerum,
				includeOracle,
				grouping, // undefined or PRICE_PRECISION
			} = req.query;

			const normedParams = normalizeBatchQueryParams({
				marketName: marketName as string | undefined,
				marketIndex: marketIndex as string | undefined,
				marketType: marketType as string | undefined,
				depth: depth as string | undefined,
				includeVamm: includeVamm as string | undefined,
				includePhoenix: includePhoenix as string | undefined,
				includeSerum: includeSerum as string | undefined,
				includeOracle: includeOracle as string | undefined,
				grouping: grouping as string | undefined,
			});

			if (normedParams === undefined) {
				res
					.status(400)
					.send(
						'Bad Request: all params for batch request must be the same length'
					);
				return;
			}

			const l2s = await Promise.all(
				normedParams.map(async (normedParam) => {
					const { normedMarketType, normedMarketIndex, error } =
						validateDlobQuery(
							driftClient,
							driftEnv,
							normedParam['marketType'] as string,
							normedParam['marketIndex'] as string,
							normedParam['marketName'] as string
						);
					if (error) {
						res.status(400).send(`Bad Request: ${error}`);
						return;
					}

					const isSpot = isVariant(normedMarketType, 'spot');

					let adjustedDepth = normedParam['depth'] ?? '10';
					if (normedParam['grouping'] !== undefined) {
						// If grouping is also supplied, we want the entire book depth.
						// we will apply depth after grouping
						adjustedDepth = '-1';
					}

					let l2Formatted: any;
					if (useRedis) {
						if (
							!isSpot &&
							normedParam['includeVamm']?.toLowerCase() === 'true' &&
							normedParam['includeOracle']?.toLowerCase() === 'true' &&
							!normedParam['grouping']
						) {
							let redisL2: string;
							if (parseInt(adjustedDepth as string) === 5) {
								redisL2 = await redisClient.client.get(
									`last_update_orderbook_perp_${normedMarketIndex}_depth_5`
								);
							} else if (parseInt(adjustedDepth as string) === 20) {
								redisL2 = await redisClient.client.get(
									`last_update_orderbook_perp_${normedMarketIndex}_depth_20`
								);
							} else if (parseInt(adjustedDepth as string) === 100) {
								redisL2 = await redisClient.client.get(
									`last_update_orderbook_perp_${normedMarketIndex}_depth_100`
								);
							}
							if (redisL2) {
								const parsedRedisL2 = JSON.parse(redisL2);
								if (slotSource.getSlot() - parseInt(parsedRedisL2.slot) < 10)
									l2Formatted = parsedRedisL2;
							}
						} else if (
							isSpot &&
							normedParam['includePhoenix']?.toLowerCase() === 'true' &&
							normedParam['includeSerum']?.toLowerCase() === 'true' &&
							!normedParam['grouping']
						) {
							let redisL2: string;
							if (parseInt(adjustedDepth as string) === 5) {
								redisL2 = await redisClient.client.get(
									`last_update_orderbook_spot_${normedMarketIndex}_depth_5`
								);
							} else if (parseInt(adjustedDepth as string) === 20) {
								redisL2 = await redisClient.client.get(
									`last_update_orderbook_spot_${normedMarketIndex}_depth_20`
								);
							} else if (parseInt(adjustedDepth as string) === 100) {
								redisL2 = await redisClient.client.get(
									`last_update_orderbook_spot_${normedMarketIndex}_depth_100`
								);
							}
							if (redisL2) {
								const parsedRedisL2 = JSON.parse(redisL2);
								if (slotSource.getSlot() - parseInt(parsedRedisL2.slot) < 10)
									l2Formatted = parsedRedisL2;
							}
						}

						if (l2Formatted) {
							cacheHitCounter.add(1, {
								miss: false,
							});
							return l2Formatted;
						}
					}

					const l2 = dlobSubscriber.getL2({
						marketIndex: normedMarketIndex,
						marketType: normedMarketType,
						depth: parseInt(adjustedDepth as string),
						includeVamm: isSpot
							? false
							: `${normedParam['includeVamm']}`.toLowerCase() === 'true',
						fallbackL2Generators: isSpot
							? [
									`${normedParam['includePhoenix']}`.toLowerCase() === 'true' &&
										MARKET_SUBSCRIBERS[normedMarketIndex].phoenix,
									`${normedParam['includeSerum']}`.toLowerCase() === 'true' &&
										MARKET_SUBSCRIBERS[normedMarketIndex].serum,
							  ].filter((a) => !!a)
							: [],
					});

					if (normedParam['grouping']) {
						const finalDepth = normedParam['depth']
							? parseInt(normedParam['depth'] as string)
							: 10;
						if (isNaN(parseInt(normedParam['grouping'] as string))) {
							res
								.status(400)
								.send('Bad Request: grouping must be a number if supplied');
							return;
						}
						const groupingBN = new BN(
							parseInt(normedParam['grouping'] as string)
						);

						l2Formatted = l2WithBNToStrings(
							groupL2(l2, groupingBN, finalDepth)
						);
						if (`${normedParam['includeOracle']}`.toLowerCase() === 'true') {
							addOracletoResponse(
								l2Formatted,
								driftClient,
								normedMarketType,
								normedMarketIndex
							);
						}
					} else {
						// make the BNs into strings
						l2Formatted = l2WithBNToStrings(l2);
						if (`${normedParam['includeOracle']}`.toLowerCase() === 'true') {
							addOracletoResponse(
								l2Formatted,
								driftClient,
								normedMarketType,
								normedMarketIndex
							);
						}
					}
					cacheHitCounter.add(1, {
						miss: true,
					});
					return l2Formatted;
				})
			);

			res.writeHead(200);
			res.end(JSON.stringify({ l2s }));
		} catch (err) {
			next(err);
		}
	});

	app.get('/l3', async (req, res, next) => {
		try {
			const { marketName, marketIndex, marketType, includeOracle } = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
				driftClient,
				driftEnv,
				marketType as string,
				marketIndex as string,
				marketName as string
			);
			if (error) {
				res.status(400).send(error);
				return;
			}

			const l3 = dlobSubscriber.getL3({
				marketIndex: normedMarketIndex,
				marketType: normedMarketType,
			});

			for (const key of Object.keys(l3)) {
				for (const idx in l3[key]) {
					const level = l3[key][idx];
					l3[key][idx] = {
						...level,
						price: level.price.toString(),
						size: level.size.toString(),
					};
				}
			}

			if (`${includeOracle}`.toLowerCase() === 'true') {
				addOracletoResponse(
					l3,
					driftClient,
					normedMarketType,
					normedMarketIndex
				);
			}

			res.writeHead(200);
			res.end(JSON.stringify(l3));
		} catch (err) {
			next(err);
		}
	});

	server.listen(serverPort, () => {
		logger.info(`DLOB server listening on port http://localhost:${serverPort}`);
	});
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

export { commitHash, driftClient, driftEnv, endpoint, sdkConfig, wsEndpoint };
