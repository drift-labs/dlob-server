import { program } from 'commander';
import express from 'express';
import rateLimit from 'express-rate-limit';
import compression from 'compression';
import morgan from 'morgan';
import cors from 'cors';

import { Connection, Commitment, PublicKey, Keypair } from '@solana/web3.js';

import {
	getVariant,
	DriftClient,
	initialize,
	DriftEnv,
	SlotSubscriber,
	UserMap,
	DLOBOrder,
	DLOBOrders,
	DLOBOrdersCoder,
	DLOBNode,
	isVariant,
	BN,
	groupL2,
	Wallet,
	UserStatsMap,
	DLOBSubscriber,
	BulkAccountLoader,
} from '@drift-labs/sdk';

import { logger, setLogLevel } from './utils/logger';

import * as http from 'http';
import {
	l2WithBNToStrings,
	sleep,
	getOracleForMarket,
	normalizeBatchQueryParams,
	SubscriberLookup,
	errorHandler,
	getPhoenixSubscriber,
	getSerumSubscriber,
	validateDlobQuery,
} from './utils/utils';
import { handleResponseTime } from './core/middleware';
import { handleHealthCheck } from './core/metrics';
import { Mutex } from 'async-mutex';

require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'processed';
const serverPort = process.env.PORT || 6969;
const ORDERBOOK_UPDATE_INTERVAL = 1000;

const rateLimitCallsPerSecond = process.env.RATE_LIMIT_CALLS_PER_SECOND
	? parseInt(process.env.RATE_LIMIT_CALLS_PER_SECOND)
	: 1;
const loadTestAllowed = process.env.ALLOW_LOAD_TEST?.toLowerCase() === 'true';

const logFormat =
	':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :req[x-forwarded-for]';
const logHttp = morgan(logFormat, {
	skip: (_req, res) => res.statusCode < 400,
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

app.use(errorHandler);
const server = http.createServer(app);

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const endpoint = process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

let MARKET_SUBSCRIBERS: SubscriberLookup = {};

const lastSlotReceivedMutex = new Mutex();
let lastSlotReceived: number;

export const getSlotHealthCheckInfo = () => {
	return { lastSlotReceived, lastSlotReceivedMutex };
};

const initializeAllMarketSubscribers = async (driftClient: DriftClient) => {
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
				sdkConfig
			);
			await phoenixSubscriber.subscribe();
			markets[market.marketIndex].phoenix = phoenixSubscriber;
		}

		if (market.serumMarket) {
			const serumSubscriber = getSerumSubscriber(
				driftClient,
				market,
				sdkConfig
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
		ORDERBOOK_UPDATE_INTERVAL
	);

	driftClient = new DriftClient({
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

	setInterval(async () => {
		await lastSlotReceivedMutex.runExclusive(async () => {
			lastSlotReceived = bulkAccountLoader.getSlot();
		});
	}, ORDERBOOK_UPDATE_INTERVAL);

	const userMap = new UserMap(
		driftClient,
		driftClient.userAccountSubscriptionConfig,
		false
	);
	await userMap.subscribe();
	const userStatsMap = new UserStatsMap(driftClient, {
		type: 'polling',
		accountLoader: new BulkAccountLoader(
			connection,
			stateCommitment,
			0
		),
	});
	await userStatsMap.subscribe();

	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: userMap,
		slotSource: bulkAccountLoader,
		updateFrequency: ORDERBOOK_UPDATE_INTERVAL,
	});
	await dlobSubscriber.subscribe();

	MARKET_SUBSCRIBERS = await initializeAllMarketSubscribers(driftClient);

	const handleStartup = async (_req, res, _next) => {
		if (
			driftClient.isSubscribed &&
			userMap.size() > 0 &&
			userStatsMap.size() > 0
		) {
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(500);
			res.end('Not ready');
		}
	};

	app.get('/health', handleHealthCheck);
	app.get('/startup', handleStartup);
	app.get('/', handleHealthCheck);

	app.get('/orders/json/raw', async (_req, res, next) => {
		try {
			// object with userAccount key and orders object serialized
			const orders: Array<any> = [];
			const oracles: Array<any> = [];
			const slot = bulkAccountLoader.getSlot();

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
			const slot = bulkAccountLoader.getSlot();
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
					slot: bulkAccountLoader.getSlot(),
					data: dlobCoder.encode(dlobOrders).toString('base64'),
				})
			);
		} catch (err) {
			next(err);
		}
	});

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
								const userAccount = side.userAccount.toBase58();
								await userMap.mustGet(userAccount);
								const userStats = await userStatsMap.mustGet(
									userMap.getUserAuthority(userAccount)!.toBase58()
								);
								topMakers.add([
									userAccount,
									userStats.userStatsAccountPublicKey.toBase58(),
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
							bulkAccountLoader.getSlot(),
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
							bulkAccountLoader.getSlot(),
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
					l2Formatted['oracle'] = getOracleForMarket(
						driftClient,
						normedMarketType,
						normedMarketIndex
					);
				}

				res.writeHead(200);
				res.end(JSON.stringify(l2Formatted));
			} else {
				// make the BNs into strings
				const l2Formatted = l2WithBNToStrings(l2);
				if (`${includeOracle}`.toLowerCase() === 'true') {
					l2Formatted['oracle'] = getOracleForMarket(
						driftClient,
						normedMarketType,
						normedMarketIndex
					);
				}

				res.writeHead(200);
				res.end(JSON.stringify(l2Formatted));
			}
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

			const l2s = normedParams.map((normedParam) => {
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

					const l2Formatted = l2WithBNToStrings(
						groupL2(l2, groupingBN, finalDepth)
					);
					if (`${normedParam['includeOracle']}`.toLowerCase() === 'true') {
						l2Formatted['oracle'] = getOracleForMarket(
							driftClient,
							normedMarketType,
							normedMarketIndex
						);
					}
					return l2Formatted;
				} else {
					// make the BNs into strings
					const l2Formatted = l2WithBNToStrings(l2);
					if (`${normedParam['includeOracle']}`.toLowerCase() === 'true') {
						l2Formatted['oracle'] = getOracleForMarket(
							driftClient,
							normedMarketType,
							normedMarketIndex
						);
					}
					return l2Formatted;
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
				l3['oracle'] = getOracleForMarket(
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

export { sdkConfig, endpoint, wsEndpoint, driftEnv, commitHash, driftClient };
