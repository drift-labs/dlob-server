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
	SlotSubscriber,
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
import { getDLOBProviderFromOrderSubscriber } from './dlobProvider';
import { RedisClient } from './utils/redisClient';

require('dotenv').config();

// Reading in Redis env vars
const REDIS_HOSTS_ENV = (process.env.REDIS_HOSTS as string) || 'localhost';
const REDIS_HOSTS = REDIS_HOSTS_ENV.includes(',')
	? REDIS_HOSTS_ENV.trim()
			.replace(/^\[|\]$/g, '')
			.split(/\s*,\s*/)
	: [REDIS_HOSTS_ENV];

const REDIS_PASSWORDS_ENV = (process.env.REDIS_PASSWORDS as string) || '';
const REDIS_PASSWORDS = REDIS_PASSWORDS_ENV.includes(',')
	? REDIS_PASSWORDS_ENV.trim()
			.replace(/^\[|\]$/g, '')
			.split(/\s*,\s*/)
	: [REDIS_PASSWORDS_ENV];

const REDIS_PORTS_ENV = (process.env.REDIS_PORTS as string) || '6379';
const REDIS_PORTS = REDIS_PORTS_ENV.includes(',')
	? REDIS_PORTS_ENV.trim()
			.replace(/^\[|\]$/g, '')
			.split(/\s*,\s*/)
	: [REDIS_PORTS_ENV];
if (
	REDIS_PORTS.length !== REDIS_PASSWORDS.length ||
	REDIS_PORTS.length !== REDIS_HOSTS.length
) {
	throw 'REDIS_HOSTS and REDIS_PASSWORDS and REDIS_PORTS must be the same length';
}

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'processed';
const serverPort = process.env.PORT || 6969;
export const ORDERBOOK_UPDATE_INTERVAL = 1000;
const WS_FALLBACK_FETCH_INTERVAL = ORDERBOOK_UPDATE_INTERVAL * 10;
const SLOT_STALENESS_TOLERANCE =
	parseInt(process.env.SLOT_STALENESS_TOLERANCE) || 20;
const useWebsocket = process.env.USE_WEBSOCKET?.toLowerCase() === 'true';
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

const main = async (): Promise<void> => {
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
	} else {
		accountSubscription = {
			type: 'websocket',
			commitment: stateCommitment,
		};
		slotSubscriber = new SlotSubscriber(connection);
		await slotSubscriber.subscribe();
	}

	driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription,
		env: driftEnv,
	});

	let subscriptionConfig;
	if (useWebsocket) {
		subscriptionConfig = {
			type: 'websocket',
			commitment: stateCommitment,
			resyncIntervalMs: WS_FALLBACK_FETCH_INTERVAL,
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

	const dlobProvider = getDLOBProviderFromOrderSubscriber(orderSubscriber);

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

	logger.info(`Initializing DLOBSubscriber...`);
	const initDlobSubscriberStart = Date.now();
	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: dlobProvider,
		slotSource: dlobProvider,
		updateFrequency: ORDERBOOK_UPDATE_INTERVAL,
	});
	await dlobSubscriber.subscribe();
	logger.info(
		`DLOBSubscriber initialized in ${Date.now() - initDlobSubscriberStart} ms`
	);

	const redisClients: Array<RedisClient> = [];
	const spotMarketRedisMap: Map<
		number,
		{ client: RedisClient; clientIndex: number }
	> = new Map();
	const perpMarketRedisMap: Map<
		number,
		{ client: RedisClient; clientIndex: number }
	> = new Map();
	if (useRedis) {
		logger.info('Connecting to redis');
		for (let i = 0; i < REDIS_HOSTS.length; i++) {
			redisClients.push(
				new RedisClient(
					REDIS_HOSTS[i],
					REDIS_PORTS[i],
					REDIS_PASSWORDS[i] || undefined
				)
			);
			await redisClients[i].connect();
		}
		for (let i = 0; i < sdkConfig.SPOT_MARKETS.length; i++) {
			spotMarketRedisMap.set(sdkConfig.SPOT_MARKETS[i].marketIndex, {
				client: redisClients[0],
				clientIndex: 0,
			});
		}
		for (let i = 0; i < sdkConfig.PERP_MARKETS.length; i++) {
			perpMarketRedisMap.set(sdkConfig.PERP_MARKETS[i].marketIndex, {
				client: redisClients[0],
				clientIndex: 0,
			});
		}
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
		handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, dlobProvider)
	);
	app.get('/startup', handleStartup);
	app.get('/', handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, dlobProvider));

	if (FEATURE_FLAGS.ENABLE_ORDERS_ENDPOINTS) {
		app.get('/orders/json/raw', async (_req, res, next) => {
			try {
				// object with userAccount key and orders object serialized
				const orders: Array<any> = [];
				const oracles: Array<any> = [];
				const slot = dlobProvider.getSlot();

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
				const slot = dlobProvider.getSlot();
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
						slot: dlobProvider.getSlot(),
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
						const maker = side.userAccount;
						if (topMakers.has(maker)) {
							continue;
						} else {
							if (`${includeUserStats}`.toLowerCase() === 'true') {
								const userAccount = dlobProvider.getUserAccount(
									new PublicKey(side.userAccount)
								);
								topMakers.add([
									userAccount,
									getUserStatsAccountPublicKey(
										driftClient.program.programId,
										userAccount.authority
									),
								]);
							} else {
								topMakers.add(side.userAccount);
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
							dlobProvider.getSlot(),
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
							dlobProvider.getSlot(),
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
					const redisClient = perpMarketRedisMap.get(normedMarketIndex).client;
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
						dlobProvider.getSlot() - parseInt(JSON.parse(redisL2).slot) <
							SLOT_STALENESS_TOLERANCE
					) {
						l2Formatted = redisL2;
					} else {
						if (redisL2 && redisClients.length > 1) {
							const nextClientIndex =
								(perpMarketRedisMap.get(normedMarketIndex).clientIndex + 1) %
								redisClients.length;
							perpMarketRedisMap.set(normedMarketIndex, {
								client: redisClients[nextClientIndex],
								clientIndex: nextClientIndex,
							});
							console.log(
								`Rotated redis client to index ${nextClientIndex} for perp market ${normedMarketIndex}`
							);
						}
					}
				} else if (
					isSpot &&
					`${includeSerum}`?.toLowerCase() === 'true' &&
					`${includePhoenix}`?.toLowerCase() === 'true' &&
					`${includeOracle}`?.toLowerCase() === 'true' &&
					!grouping
				) {
					let redisL2: string;
					const redisClient = spotMarketRedisMap.get(normedMarketIndex).client;
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
						dlobProvider.getSlot() - parseInt(JSON.parse(redisL2).slot) <
							SLOT_STALENESS_TOLERANCE
					) {
						l2Formatted = redisL2;
					} else {
						if (redisL2 && redisClients.length > 1) {
							const nextClientIndex =
								(spotMarketRedisMap.get(normedMarketIndex).clientIndex + 1) %
								redisClients.length;
							spotMarketRedisMap.set(normedMarketIndex, {
								client: redisClients[nextClientIndex],
								clientIndex: nextClientIndex,
							});
							console.log(
								`Rotated redis client to index ${nextClientIndex} for spot market ${normedMarketIndex}`
							);
						}
					}
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
							const redisClient =
								perpMarketRedisMap.get(normedMarketIndex).client;
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
								if (
									dlobProvider.getSlot() - parseInt(parsedRedisL2.slot) <
									SLOT_STALENESS_TOLERANCE
								) {
									l2Formatted = parsedRedisL2;
								} else {
									if (redisClients.length > 1) {
										const nextClientIndex =
											(perpMarketRedisMap.get(normedMarketIndex).clientIndex +
												1) %
											redisClients.length;
										perpMarketRedisMap.set(normedMarketIndex, {
											client: redisClients[nextClientIndex],
											clientIndex: nextClientIndex,
										});
										console.log(
											`Rotated redis client to index ${nextClientIndex} for perp market ${normedMarketIndex}`
										);
									}
								}
							}
						} else if (
							isSpot &&
							normedParam['includePhoenix']?.toLowerCase() === 'true' &&
							normedParam['includeSerum']?.toLowerCase() === 'true' &&
							!normedParam['grouping']
						) {
							let redisL2: string;
							const redisClient =
								spotMarketRedisMap.get(normedMarketIndex).client;
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
								if (
									dlobProvider.getSlot() - parseInt(parsedRedisL2.slot) <
									SLOT_STALENESS_TOLERANCE
								) {
									l2Formatted = parsedRedisL2;
								} else {
									if (redisClients.length > 1) {
										const nextClientIndex =
											(spotMarketRedisMap.get(normedMarketIndex).clientIndex +
												1) %
											redisClients.length;
										spotMarketRedisMap.set(normedMarketIndex, {
											client: redisClients[nextClientIndex],
											clientIndex: nextClientIndex,
										});
										console.log(
											`Rotated redis client to index ${nextClientIndex} for spot market ${normedMarketIndex}`
										);
									}
								}
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

async function recursiveTryCatch(f: () => Promise<void>) {
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
