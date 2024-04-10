import { program } from 'commander';
import compression from 'compression';
import cors from 'cors';
import express from 'express';
import morgan from 'morgan';

import { Commitment, Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	BulkAccountLoader,
	DLOBNode,
	DLOBSubscriber,
	DriftClient,
	DriftClientSubscriptionConfig,
	DriftEnv,
	SlotSubscriber,
	Wallet,
	getVariant,
	initialize,
	isVariant,
	OrderSubscriber,
	MarketType,
} from '@drift-labs/sdk';

import { logger, setLogLevel } from './utils/logger';

import * as http from 'http';
import {
	handleHealthCheck,
	accountUpdatesCounter,
	cacheHitCounter,
	setLastReceivedWsMsgTs,
	incomingRequestsCounter,
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
const REDIS_HOSTS = process.env.REDIS_HOSTS?.replace(/^\[|\]$/g, '')
	.split(',')
	.map((host) => host.trim()) || ['localhost'];
const REDIS_PORTS = process.env.REDIS_PORTS?.replace(/^\[|\]$/g, '')
	.split(',')
	.map((port) => parseInt(port.trim(), 10)) || [6379];
const REDIS_PASSWORDS_ENV = process.env.REDIS_PASSWORDS || "['']";

let REDIS_PASSWORDS;

if (REDIS_PASSWORDS_ENV.trim() === "['']") {
	REDIS_PASSWORDS = [undefined];
} else {
	REDIS_PASSWORDS = REDIS_PASSWORDS_ENV.replace(/^\[|\]$/g, '')
		.split(/\s*,\s*/)
		.map((pwd) => pwd.replace(/(^'|'$)/g, '').trim())
		.map((pwd) => (pwd === '' ? undefined : pwd));
}

console.log('Redis Hosts:', REDIS_HOSTS);
console.log('Redis Ports:', REDIS_PORTS);
console.log('Redis Passwords:', REDIS_PASSWORDS);

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

const stateCommitment: Commitment = 'confirmed';
const serverPort = process.env.PORT || 6969;
export const ORDERBOOK_UPDATE_INTERVAL = 1000;
const WS_FALLBACK_FETCH_INTERVAL = ORDERBOOK_UPDATE_INTERVAL * 10;
const SLOT_STALENESS_TOLERANCE =
	parseInt(process.env.SLOT_STALENESS_TOLERANCE) || 20;
const ROTATION_COOLDOWN = parseInt(process.env.ROTATION_COOLDOWN) || 5000;
const useWebsocket = process.env.USE_WEBSOCKET?.toLowerCase() === 'true';
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

// strip off /dlob, if the request comes from exchange history server LB
app.use((req, _res, next) => {
	if (req.url.startsWith('/dlob')) {
		req.url = req.url.replace('/dlob', '');
		if (req.url === '') {
			req.url = '/';
		}
	}
	incomingRequestsCounter.add(1);
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
		slotSubscriber = new SlotSubscriber(connection, {
			resubTimeoutMs: 5000,
		});
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

	// Handle redis client initialization and rotation maps
	const redisClients: Array<RedisClient> = [];
	const spotMarketRedisMap: Map<
		number,
		{
			client: RedisClient;
			clientIndex: number;
			lastRotationTime: number;
			lock: boolean;
		}
	> = new Map();
	const perpMarketRedisMap: Map<
		number,
		{
			client: RedisClient;
			clientIndex: number;
			lastRotationTime: number;
			lock: boolean;
		}
	> = new Map();
	if (useRedis) {
		logger.info('Connecting to redis');
		for (let i = 0; i < REDIS_HOSTS.length; i++) {
			redisClients.push(
				new RedisClient(
					REDIS_HOSTS[i],
					REDIS_PORTS[i].toString(),
					REDIS_PASSWORDS[i] || undefined
				)
			);
			await redisClients[i].connect();
		}
		for (let i = 0; i < sdkConfig.SPOT_MARKETS.length; i++) {
			spotMarketRedisMap.set(sdkConfig.SPOT_MARKETS[i].marketIndex, {
				client: redisClients[0],
				clientIndex: 0,
				lastRotationTime: 0,
				lock: false,
			});
		}
		for (let i = 0; i < sdkConfig.PERP_MARKETS.length; i++) {
			perpMarketRedisMap.set(sdkConfig.PERP_MARKETS[i].marketIndex, {
				client: redisClients[0],
				clientIndex: 0,
				lastRotationTime: 0,
				lock: false,
			});
		}
	}

	function canRotate(marketType: MarketType, marketIndex: number) {
		if (isVariant(marketType, 'spot')) {
			const state = spotMarketRedisMap.get(marketIndex);
			if (state) {
				const now = Date.now();
				if (now - state.lastRotationTime > ROTATION_COOLDOWN && !state.lock) {
					state.lastRotationTime = now;
					return true;
				}
			}
		} else {
			const state = perpMarketRedisMap.get(marketIndex);
			if (state) {
				const now = Date.now();
				if (now - state.lastRotationTime > ROTATION_COOLDOWN && !state.lock) {
					state.lastRotationTime = now;
					return true;
				}
			}
		}
		return false;
	}

	function rotateClient(marketType: MarketType, marketIndex: number) {
		if (isVariant(marketType, 'spot')) {
			const state = spotMarketRedisMap.get(marketIndex);
			if (state) {
				state.lock = true;
				const nextClientIndex = (state.clientIndex + 1) % redisClients.length;
				state.client = redisClients[nextClientIndex];
				state.clientIndex = nextClientIndex;
				logger.info(
					`Rotated redis client to index ${nextClientIndex} for spot market ${marketIndex}`
				);
				state.lastRotationTime = Date.now();
				state.lock = false;
			}
		} else {
			const state = perpMarketRedisMap.get(marketIndex);
			if (state) {
				state.lock = true;
				const nextClientIndex = (state.clientIndex + 1) % redisClients.length;
				state.client = redisClients[nextClientIndex];
				state.clientIndex = nextClientIndex;
				logger.info(
					`Rotated redis client to index ${nextClientIndex} for perp market ${marketIndex}`
				);
				state.lastRotationTime = Date.now();
				state.lock = false;
			}
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

	app.get('/priorityFees', async (req, res, next) => {
		try {
			const { marketIndex, marketType } = req.query;

			const fees = await redisClients[
				parseInt(process.env.HELIUS_REDIS_HOST_INDEX) ?? 0
			].client.get(`priorityFees_${marketType}_${marketIndex}`);
			if (fees) {
				res.status(200).json({
					...JSON.parse(fees),
					marketType,
					marketIndex,
				});
				return;
			} else {
				res.writeHead(404);
				res.end('Not found');
			}
		} catch (err) {
			next(err);
		}
	});

	app.get('/batchPriorityFees', async (req, res, next) => {
		try {
			const { marketIndex, marketType } = req.query;

			const normedParams = normalizeBatchQueryParams({
				marketIndex: marketIndex as string | undefined,
				marketType: marketType as string | undefined,
			});

			if (normedParams === undefined) {
				res
					.status(400)
					.send(
						'Bad Request: all params for batch request must be the same length'
					);
				return;
			}

			const fees = await Promise.all(
				normedParams.map(async (normedParam) => {
					const fees = await redisClients[
						parseInt(process.env.HELIUS_REDIS_HOST_INDEX) ?? 0
					].client.get(
						`priorityFees_${normedParam['marketType']}_${normedParam['marketIndex']}`
					);
					return {
						...JSON.parse(fees),
						marketType,
						marketIndex,
					};
				})
			);

			if (fees && fees.length > 0) {
				res.status(200).json(fees);
				return;
			} else {
				res.writeHead(404);
				res.end('Not found');
			}
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
				limit,
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
			} else {
				normedLimit = 4;
			}

			let topMakers: string[];
			if (useRedis) {
				const redisClient = isVariant(normedMarketType, 'perp')
					? perpMarketRedisMap.get(normedMarketIndex).client
					: spotMarketRedisMap.get(normedMarketIndex).client;
				const redisResponse = await redisClient.client.get(
					`last_update_orderbook_best_makers_${getVariant(
						normedMarketType
					)}_${marketIndex}`
				);
				if (redisResponse) {
					const parsedResponse = JSON.parse(redisResponse);
					if (
						parsedResponse &&
						dlobProvider.getSlot() - parsedResponse.slot <
							SLOT_STALENESS_TOLERANCE
					) {
						if (side === 'bid') {
							topMakers = parsedResponse.bids;
						} else {
							topMakers = parsedResponse.asks;
						}
					}
				}
			}

			if (topMakers) {
				cacheHitCounter.add(1, {
					miss: false,
					path: req.baseUrl + req.path,
				});
				res.writeHead(200);
				res.end(JSON.stringify(topMakers));
				return;
			}

			const topMakersSet = new Set<string>();
			let foundMakers = 0;
			const findMakers = async (sideGenerator: Generator<DLOBNode>) => {
				for (const side of sideGenerator) {
					if (limit && foundMakers >= normedLimit) {
						break;
					}
					if (side.userAccount) {
						const maker = side.userAccount;
						if (topMakersSet.has(maker)) {
							continue;
						} else {
							topMakersSet.add(side.userAccount);
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
			topMakers = [...topMakersSet];
			cacheHitCounter.add(1, {
				miss: true,
				path: req.baseUrl + req.path,
			});
			res.writeHead(200);
			res.end(JSON.stringify(topMakers));
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

			const adjustedDepth = depth ?? '100';

			let l2Formatted: any;
			if (useRedis) {
				if (
					!isSpot &&
					`${includeVamm}`?.toLowerCase() === 'true' &&
					`${includeOracle}`?.toLowerCase() === 'true'
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
							if (canRotate(normedMarketType, normedMarketIndex)) {
								rotateClient(normedMarketType, normedMarketIndex);
							}
						}
					}
				} else if (
					isSpot &&
					`${includeSerum}`?.toLowerCase() === 'true' &&
					`${includePhoenix}`?.toLowerCase() === 'true' &&
					`${includeOracle}`?.toLowerCase() === 'true'
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
							if (canRotate(normedMarketType, normedMarketIndex)) {
								rotateClient(normedMarketType, normedMarketIndex);
							}
						}
					}
				}

				if (l2Formatted) {
					cacheHitCounter.add(1, {
						miss: false,
						path: req.baseUrl + req.path,
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

			cacheHitCounter.add(1, {
				miss: true,
				path: req.baseUrl + req.path,
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
			});

			if (normedParams === undefined) {
				res
					.status(400)
					.send(
						'Bad Request: all params for batch request must be the same length'
					);
				return;
			}

			let hasError = false;
			let errorMessage = '';
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
						hasError = true;
						errorMessage = `Bad Request: ${error}`;
						return;
					}

					const isSpot = isVariant(normedMarketType, 'spot');

					const adjustedDepth = normedParam['depth'] ?? '100';
					let l2Formatted: any;
					if (useRedis) {
						if (
							!isSpot &&
							normedParam['includeVamm']?.toLowerCase() === 'true' &&
							normedParam['includeOracle']?.toLowerCase() === 'true'
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
										if (canRotate(normedMarketType, normedMarketIndex)) {
											rotateClient(normedMarketType, normedMarketIndex);
										}
									}
								}
							}
						} else if (
							isSpot &&
							normedParam['includePhoenix']?.toLowerCase() === 'true' &&
							normedParam['includeSerum']?.toLowerCase() === 'true'
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
										if (canRotate(normedMarketType, normedMarketIndex)) {
											rotateClient(normedMarketType, normedMarketIndex);
										}
									}
								}
							}
						}

						if (l2Formatted) {
							cacheHitCounter.add(1, {
								miss: false,
								path: req.baseUrl + req.path,
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
					cacheHitCounter.add(1, {
						miss: true,
						path: req.baseUrl + req.path,
					});
					return l2Formatted;
				})
			);

			if (hasError) {
				res.status(400).send(errorMessage);
				return;
			}

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

			const marketTypeStr = getVariant(normedMarketType);
			if (useRedis) {
				const redisClient = (
					marketTypeStr === 'spot' ? spotMarketRedisMap : perpMarketRedisMap
				).get(normedMarketIndex).client;
				const redisL3 = await redisClient.client.get(
					`last_update_orderbook_l3_${marketTypeStr}_${normedMarketIndex}`
				);
				if (
					redisL3 &&
					dlobProvider.getSlot() - parseInt(JSON.parse(redisL3).slot) <
						SLOT_STALENESS_TOLERANCE
				) {
					cacheHitCounter.add(1, {
						miss: false,
						path: req.baseUrl + req.path,
					});
					res.writeHead(200);
					res.end(redisL3);
					return;
				} else {
					cacheHitCounter.add(1, {
						miss: true,
						path: req.baseUrl + req.path,
					});
				}
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
