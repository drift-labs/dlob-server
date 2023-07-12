import { program } from 'commander';

import responseTime = require('response-time');
import express, { Request, Response } from 'express';
import rateLimit from 'express-rate-limit';
import compression from 'compression';
import morgan from 'morgan';
import cors from 'cors';

import { Connection, Commitment, PublicKey, Keypair } from '@solana/web3.js';

import {
	getVariant,
	BulkAccountLoader,
	DriftClient,
	initialize,
	DriftEnv,
	SlotSubscriber,
	UserMap,
	DLOBOrder,
	DLOBOrders,
	DLOBOrdersCoder,
	SpotMarkets,
	PerpMarkets,
	DLOBSubscriber,
	MarketType,
	SpotMarketConfig,
	PhoenixSubscriber,
	SerumSubscriber,
	DLOBNode,
	isVariant,
	BN,
	groupL2,
	L2OrderBook,
	Wallet,
} from '@drift-labs/sdk';

import { Mutex } from 'async-mutex';

import { logger, setLogLevel } from './logger';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { ObservableResult } from '@opentelemetry/api';

require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
const serverPort = process.env.PORT || 6969;

const bulkAccountLoaderPollingInterval = process.env
	.BULK_ACCOUNT_LOADER_POLLING_INTERVAL
	? parseInt(process.env.BULK_ACCOUNT_LOADER_POLLING_INTERVAL)
	: 5000;
const healthCheckInterval = bulkAccountLoaderPollingInterval * 2;

const rateLimitCallsPerSecond = process.env.RATE_LIMIT_CALLS_PER_SECOND
	? parseInt(process.env.RATE_LIMIT_CALLS_PER_SECOND)
	: 1;

const loadTestAllowed = process.env.ALLOW_LOAD_TEST?.toLowerCase() === 'true';

const logFormat =
	':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :req[x-forwarded-for]';
const logHttp = morgan(logFormat, {
	skip: (_req, res) => res.statusCode < 400,
});

function errorHandler(err, _req, res, _next) {
	logger.error(err.stack);
	res.status(500).send('Internal error');
}

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);
app.use(logHttp);

const handleResponseTime = responseTime(
	(req: Request, res: Response, time: number) => {
		const endpoint = req.path;

		if (endpoint === '/health' || req.url === '/') {
			return;
		}

		responseStatusCounter.add(1, {
			endpoint,
			status: res.statusCode,
		});

		const responseTimeMs = time;
		endpointResponseTimeHistogram.record(responseTimeMs, {
			endpoint,
		});
	}
);
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

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const endpoint = process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Creates {count} buckets of size {increment} starting from {start}. Each bucket stores the count of values within its "size".
 * @param start
 * @param increment
 * @param count
 * @returns
 */
const createHistogramBuckets = (
	start: number,
	increment: number,
	count: number
) => {
	return new ExplicitBucketHistogramAggregation(
		Array.from(new Array(count), (_, i) => start + i * increment)
	);
};

enum METRIC_TYPES {
	runtime_specs = 'runtime_specs',
	endpoint_response_times_histogram = 'endpoint_response_times_histogram',
	endpoint_response_status = 'endpoint_response_status',
	health_status = 'health_status',
}

export enum HEALTH_STATUS {
	Ok = 0,
	StaleBulkAccountLoader,
	UnhealthySlotSubscriber,
	LivenessTesting,
}

const metricsPort =
	parseInt(process.env.METRICS_PORT) || PrometheusExporter.DEFAULT_OPTIONS.port;
const { endpoint: defaultEndpoint } = PrometheusExporter.DEFAULT_OPTIONS;
const exporter = new PrometheusExporter(
	{
		port: metricsPort,
		endpoint: defaultEndpoint,
	},
	() => {
		logger.info(
			`prometheus scrape endpoint started: http://localhost:${metricsPort}${defaultEndpoint}`
		);
	}
);
const meterName = 'dlob-meter';
const meterProvider = new MeterProvider({
	views: [
		new View({
			instrumentName: METRIC_TYPES.endpoint_response_times_histogram,
			instrumentType: InstrumentType.HISTOGRAM,
			meterName,
			aggregation: createHistogramBuckets(0, 20, 30),
		}),
	],
});
meterProvider.addMetricReader(exporter);
const meter = meterProvider.getMeter(meterName);

const runtimeSpecsGauge = meter.createObservableGauge(
	METRIC_TYPES.runtime_specs,
	{
		description: 'Runtime sepcification of this program',
	}
);
const bootTimeMs = Date.now();
runtimeSpecsGauge.addCallback((obs) => {
	obs.observe(bootTimeMs, {
		commit: commitHash,
		driftEnv,
		rpcEndpoint: endpoint,
		wsEndpoint: wsEndpoint,
	});
});

let healthStatus: HEALTH_STATUS = HEALTH_STATUS.Ok;
const healthStatusGauge = meter.createObservableGauge(
	METRIC_TYPES.health_status,
	{
		description: 'Health status of this program',
	}
);
healthStatusGauge.addCallback((obs: ObservableResult) => {
	obs.observe(healthStatus, {});
});

const endpointResponseTimeHistogram = meter.createHistogram(
	METRIC_TYPES.endpoint_response_times_histogram,
	{
		description: 'Duration of endpoint responses',
		unit: 'ms',
	}
);

const responseStatusCounter = meter.createCounter(
	METRIC_TYPES.endpoint_response_status,
	{
		description: 'Count of endpoint responses by status code',
	}
);

const getPhoenixSubscriber = (
	driftClient: DriftClient,
	marketConfig: SpotMarketConfig,
	accountLoader: BulkAccountLoader
) => {
	return new PhoenixSubscriber({
		connection: driftClient.connection,
		programId: new PublicKey(sdkConfig.PHOENIX),
		marketAddress: marketConfig.phoenixMarket,
		accountSubscription: {
			type: 'polling',
			accountLoader,
		},
	});
};

const getSerumSubscriber = (
	driftClient: DriftClient,
	marketConfig: SpotMarketConfig,
	accountLoader: BulkAccountLoader
) => {
	return new SerumSubscriber({
		connection: driftClient.connection,
		programId: new PublicKey(sdkConfig.SERUM_V3),
		marketAddress: marketConfig.serumMarket,
		accountSubscription: {
			type: 'polling',
			accountLoader,
		},
	});
};

type SubscriberLookup = {
	[marketIndex: number]: {
		phoenix?: PhoenixSubscriber;
		serum?: SerumSubscriber;
	};
};

let MARKET_SUBSCRIBERS: SubscriberLookup = {};

const initializeAllMarketSubscribers = async (
	driftClient: DriftClient,
	bulkAccountLoader: BulkAccountLoader
) => {
	const markets: SubscriberLookup = {};

	for (const market of sdkConfig.SPOT_MARKETS) {
		markets[market.marketIndex] = {
			phoenix: undefined,
			serum: undefined,
		};

		if (market.phoenixMarket) {
			const phoenixSubscriber = getPhoenixSubscriber(
				driftClient,
				market,
				bulkAccountLoader
			);
			await phoenixSubscriber.subscribe();
			markets[market.marketIndex].phoenix = phoenixSubscriber;
		}

		if (market.serumMarket) {
			const serumSubscriber = getSerumSubscriber(
				driftClient,
				market,
				bulkAccountLoader
			);
			await serumSubscriber.subscribe();
			markets[market.marketIndex].serum = serumSubscriber;
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

	const bulkAccountLoader = new BulkAccountLoader(
		connection,
		stateCommitment,
		bulkAccountLoaderPollingInterval
	);
	const lastBulkAccountLoaderSlotMutex = new Mutex();
	let lastBulkAccountLoaderSlot = bulkAccountLoader.mostRecentSlot;
	let lastBulkAccountLoaderSlotUpdated = Date.now();
	const driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription: {
			type: 'polling',
			accountLoader: bulkAccountLoader,
		},
		env: driftEnv,
		userStats: true,
	});

	const dlobCoder = DLOBOrdersCoder.create();
	const slotSubscriber = new SlotSubscriber(connection, {});
	const lastSlotReceivedMutex = new Mutex();
	let lastSlotReceived: number;
	let lastHealthCheckSlot = -1;
	let lastHealthCheckSlotUpdated = Date.now();
	const startupTime = Date.now();

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

	await slotSubscriber.subscribe();
	slotSubscriber.eventEmitter.on('newSlot', async (slot: number) => {
		await lastSlotReceivedMutex.runExclusive(async () => {
			lastSlotReceived = slot;
		});
	});

	const userMap = new UserMap(
		driftClient,
		driftClient.userAccountSubscriptionConfig,
		false
	);
	await userMap.subscribe();

	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: userMap,
		slotSource: slotSubscriber,
		updateFrequency: bulkAccountLoaderPollingInterval,
	});
	await dlobSubscriber.subscribe();

	MARKET_SUBSCRIBERS = await initializeAllMarketSubscribers(
		driftClient,
		bulkAccountLoader
	);

	const handleHealthCheck = async (req, res, next) => {
		try {
			if (req.url === '/health' || req.url === '/') {
				if (opts.testLiveness) {
					if (Date.now() > startupTime + 60 * 1000) {
						healthStatus = HEALTH_STATUS.LivenessTesting;

						res.writeHead(500);
						res.end('Testing liveness test fail');
						return;
					}
				}
				// check if a slot was received recently
				let healthySlotSubscriber = false;
				await lastSlotReceivedMutex.runExclusive(async () => {
					const slotChanged = lastSlotReceived > lastHealthCheckSlot;
					const slotChangedRecently =
						Date.now() - lastHealthCheckSlotUpdated < healthCheckInterval;
					healthySlotSubscriber = slotChanged || slotChangedRecently;
					logger.debug(
						`Slotsubscriber health check: lastSlotReceived: ${lastSlotReceived}, lastHealthCheckSlot: ${lastHealthCheckSlot}, slotChanged: ${slotChanged}, slotChangedRecently: ${slotChangedRecently}`
					);
					if (slotChanged) {
						lastHealthCheckSlot = lastSlotReceived;
						lastHealthCheckSlotUpdated = Date.now();
					}
				});
				if (!healthySlotSubscriber) {
					healthStatus = HEALTH_STATUS.UnhealthySlotSubscriber;
					logger.error(`SlotSubscriber is not healthy`);

					res.writeHead(500);
					res.end(`SlotSubscriber is not healthy`);
					return;
				}

				if (bulkAccountLoader) {
					let healthyBulkAccountLoader = false;
					await lastBulkAccountLoaderSlotMutex.runExclusive(async () => {
						const slotChanged =
							bulkAccountLoader.mostRecentSlot > lastBulkAccountLoaderSlot;
						const slotChangedRecently =
							Date.now() - lastBulkAccountLoaderSlotUpdated <
							healthCheckInterval;
						healthyBulkAccountLoader = slotChanged || slotChangedRecently;
						logger.debug(
							`BulkAccountLoader health check: bulkAccountLoader.mostRecentSlot: ${bulkAccountLoader.mostRecentSlot}, lastBulkAccountLoaderSlot: ${lastBulkAccountLoaderSlot}, slotChanged: ${slotChanged}, slotChangedRecently: ${slotChangedRecently}`
						);
						if (slotChanged) {
							lastBulkAccountLoaderSlot = bulkAccountLoader.mostRecentSlot;
							lastBulkAccountLoaderSlotUpdated = Date.now();
						}
					});
					if (!healthyBulkAccountLoader) {
						healthStatus = HEALTH_STATUS.StaleBulkAccountLoader;
						logger.error(
							`Health check failed due to stale bulkAccountLoader.mostRecentSlot`
						);

						res.writeHead(501);
						res.end(`bulkAccountLoader.mostRecentSlot is not healthy`);
						return;
					}
				}

				// liveness check passed
				healthStatus = HEALTH_STATUS.Ok;
				res.writeHead(200);
				res.end('OK');
			} else {
				res.writeHead(404);
				res.end('Not found');
			}
		} catch (e) {
			next(e);
		}
	};

	// start http server listening to /health endpoint using http package
	app.get('/health', handleHealthCheck);
	app.get('/', handleHealthCheck);

	app.get('/orders/json/raw', async (_req, res, next) => {
		try {
			// object with userAccount key and orders object serialized
			const orders: Array<any> = [];
			const oracles: Array<any> = [];
			const slot = bulkAccountLoader.mostRecentSlot;

			for (const market of driftClient.getPerpMarketAccounts()) {
				const oracle = driftClient.getOracleDataForPerpMarket(
					market.marketIndex
				);
				oracles.push({
					marketIndex: market.marketIndex,
					...oracle,
				});
			}

			for (const user of userMap.values()) {
				const userAccount = user.getUserAccount();

				for (const order of userAccount.orders) {
					if (isVariant(order.status, 'init')) {
						continue;
					}

					orders.push({
						user: user.getUserAccountPublicKey().toBase58(),
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
			const slot = bulkAccountLoader.mostRecentSlot;
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
			for (const user of userMap.values()) {
				const userAccount = user.getUserAccount();

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
					if (order.quoteAssetAmount) {
						orderHuman['quoteAssetAmount'] = order.quoteAssetAmount.toString();
					}

					orders.push({
						user: user.getUserAccountPublicKey().toBase58(),
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

			for (const user of userMap.values()) {
				const userAccount = user.getUserAccount();

				for (const order of userAccount.orders) {
					if (isVariant(order.status, 'init')) {
						continue;
					}

					dlobOrders.push({
						user: user.getUserAccountPublicKey(),
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
			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
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

			for (const user of userMap.values()) {
				const userAccount = user.getUserAccount();

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
						user: user.getUserAccountPublicKey(),
						order,
					} as DLOBOrder);
				}
			}

			res.end(
				JSON.stringify({
					slot: bulkAccountLoader.mostRecentSlot,
					data: dlobCoder.encode(dlobOrders).toString('base64'),
				})
			);
		} catch (err) {
			next(err);
		}
	});

	const validateDlobQuery = (
		marketType?: string,
		marketIndex?: string,
		marketName?: string
	): {
		normedMarketType?: MarketType;
		normedMarketIndex?: number;
		error?: string;
	} => {
		let normedMarketType: MarketType = undefined;
		let normedMarketIndex: number = undefined;
		let normedMarketName: string = undefined;
		if (marketName === undefined) {
			if (marketIndex === undefined || marketType === undefined) {
				return {
					error:
						'Bad Request: (marketName) or (marketIndex and marketType) must be supplied',
				};
			}

			// validate marketType
			switch ((marketType as string).toLowerCase()) {
				case 'spot': {
					normedMarketType = MarketType.SPOT;
					normedMarketIndex = parseInt(marketIndex as string);
					const spotMarketIndicies = SpotMarkets[driftEnv].map(
						(mkt) => mkt.marketIndex
					);
					if (!spotMarketIndicies.includes(normedMarketIndex)) {
						return {
							error: 'Bad Request: invalid marketIndex',
						};
					}
					break;
				}
				case 'perp': {
					normedMarketType = MarketType.PERP;
					normedMarketIndex = parseInt(marketIndex as string);
					const perpMarketIndicies = PerpMarkets[driftEnv].map(
						(mkt) => mkt.marketIndex
					);
					if (!perpMarketIndicies.includes(normedMarketIndex)) {
						return {
							error: 'Bad Request: invalid marketIndex',
						};
					}
					break;
				}
				default:
					return {
						error: 'Bad Request: marketType must be either "spot" or "perp"',
					};
			}
		} else {
			// validate marketName
			normedMarketName = (marketName as string).toUpperCase();
			const derivedMarketInfo =
				driftClient.getMarketIndexAndType(normedMarketName);
			if (!derivedMarketInfo) {
				return {
					error: 'Bad Request: unrecognized marketName',
				};
			}
			normedMarketType = derivedMarketInfo.marketType;
			normedMarketIndex = derivedMarketInfo.marketIndex;
		}

		return {
			normedMarketType,
			normedMarketIndex,
		};
	};

	app.get('/topMakers', async (req, res, next) => {
		try {
			const {
				marketName,
				marketIndex,
				marketType,
				side, // bid or ask
				limit, // number of unique makers to return, if undefined will return all
			} = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
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

			const topMakers: Set<string> = new Set();
			let foundMakers = 0;
			const findMakers = (sideGenerator: Generator<DLOBNode>) => {
				for (const side of sideGenerator) {
					if (limit && foundMakers >= normedLimit) {
						break;
					}
					if (side.userAccount) {
						const maker = side.userAccount.toBase58();
						if (topMakers.has(maker)) {
							continue;
						} else {
							topMakers.add(side.userAccount.toBase58());
							foundMakers++;
						}
					} else {
						continue;
					}
				}
			};

			if (normedSide === 'bid') {
				findMakers(
					dlobSubscriber
						.getDLOB()
						.getRestingLimitBids(
							normedMarketIndex,
							slotSubscriber.getSlot(),
							normedMarketType,
							oracle
						)
				);
			} else {
				findMakers(
					dlobSubscriber
						.getDLOB()
						.getRestingLimitAsks(
							normedMarketIndex,
							slotSubscriber.getSlot(),
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

	const l2WithBNToStrings = (l2: L2OrderBook): any => {
		for (const key of Object.keys(l2)) {
			for (const idx in l2[key]) {
				const level = l2[key][idx];
				const sources = level['sources'];
				for (const sourceKey of Object.keys(sources)) {
					sources[sourceKey] = sources[sourceKey].toString();
				}
				l2[key][idx] = {
					price: level.price.toString(),
					size: level.size.toString(),
					sources,
				};
			}
		}
		return l2;
	};

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
			} = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
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

			const l2 = dlobSubscriber.getL2({
				marketIndex: normedMarketIndex,
				marketType: normedMarketType,
				depth: parseInt(adjustedDepth as string),
				includeVamm: `${includeVamm}`.toLowerCase() === 'true',
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
				res.writeHead(200);
				res.end(
					JSON.stringify(l2WithBNToStrings(groupL2(l2, groupingBN, finalDepth)))
				);
			} else {
				// make the BNs into strings
				res.writeHead(200);
				res.end(JSON.stringify(l2WithBNToStrings(l2)));
			}
		} catch (err) {
			next(err);
		}
	});

	/**
	 * Takes in a req.query like: `{
	 * 		marketName: 'SOL-PERP,BTC-PERP,ETH-PERP',
	 * 		marketType: undefined,
	 * 		marketIndices: undefined,
	 * 		...
	 * 	}` and returns a normalized object like:
	 *
	 * `[
	 * 		{marketName: 'SOL-PERP', marketType: undefined, marketIndex: undefined,...},
	 * 		{marketName: 'BTC-PERP', marketType: undefined, marketIndex: undefined,...},
	 * 		{marketName: 'ETH-PERP', marketType: undefined, marketIndex: undefined,...}
	 * ]`
	 *
	 * @param rawParams req.query object
	 * @returns normalized query params for batch requests, or undefined if there is a mismatched length
	 */
	const normalizeBatchQueryParams = (rawParams: {
		[key: string]: string | undefined;
	}): Array<{ [key: string]: string | undefined }> => {
		const normedParams: Array<{ [key: string]: string | undefined }> = [];
		const parsedParams = {};

		// parse the query string into arrays
		for (const key of Object.keys(rawParams)) {
			const rawParam = rawParams[key];
			if (rawParam === undefined) {
				parsedParams[key] = [];
			} else {
				parsedParams[key] = rawParam.split(',') || [rawParam];
			}
		}

		// of all parsedParams, find the max length
		const maxLength = Math.max(
			...Object.values(parsedParams).map(
				(param: Array<unknown>) => param.length
			)
		);

		// all params have to be either 0 length, or maxLength to be valid
		const values = Object.values(parsedParams);
		const validParams = values.every(
			(value: Array<unknown>) =>
				value.length === 0 || value.length === maxLength
		);
		if (!validParams) {
			return undefined;
		}

		// merge all params into an array of objects
		// normalize all params to the same length, filling in undefineds
		for (let i = 0; i < maxLength; i++) {
			const newParam = {};
			for (const key of Object.keys(parsedParams)) {
				const parsedParam = parsedParams[key];
				newParam[key] =
					parsedParam.length === maxLength ? parsedParam[i] : undefined;
			}
			normedParams.push(newParam);
		}

		return normedParams;
	};

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

			const l2s = normedParams.map((normedParam) => {
				const { normedMarketType, normedMarketIndex, error } =
					validateDlobQuery(
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

				const l2 = dlobSubscriber.getL2({
					marketIndex: normedMarketIndex,
					marketType: normedMarketType,
					depth: parseInt(adjustedDepth as string),
					includeVamm: `${normedParam['includeVamm']}`.toLowerCase() === 'true',
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

					return l2WithBNToStrings(groupL2(l2, groupingBN, finalDepth));
				} else {
					// make the BNs into strings
					return l2WithBNToStrings(l2);
				}
			});

			res.writeHead(200);
			res.end(JSON.stringify({ l2s }));
		} catch (err) {
			next(err);
		}
	});

	app.get('/l3', async (req, res, next) => {
		try {
			const { marketName, marketIndex, marketType } = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
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

			res.writeHead(200);
			res.end(JSON.stringify(l3));
		} catch (err) {
			next(err);
		}
	});

	app.use(errorHandler);
	app.listen(serverPort, () => {
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
