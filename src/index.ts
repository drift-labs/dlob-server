import { program } from 'commander';
import compression from 'compression';
import cors from 'cors';
import express from 'express';
import morgan from 'morgan';

import { Commitment, Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	DLOBNode,
	DLOBSubscriber,
	DriftClient,
	DriftEnv,
	SlotSubscriber,
	Wallet,
	getVariant,
	initialize,
	isVariant,
	OrderSubscriber,
	PhoenixSubscriber,
	BulkAccountLoader,
	isOperationPaused,
	PerpOperation,
	DelistedMarketSetting,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';

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
	l2WithBNToStrings,
	normalizeBatchQueryParams,
	sleep,
	validateDlobQuery,
	getAccountFromId,
	getRawAccountFromId,
	getOpenbookSubscriber,
	selectMostRecentBySlot,
} from './utils/utils';
import FEATURE_FLAGS from './utils/featureFlags';
import { getDLOBProviderFromOrderSubscriber } from './dlobProvider';
import { setGlobalDispatcher, Agent } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();

const envClients = [];
const clients = process.env.REDIS_CLIENT?.trim()
	.replace(/^\[|\]$/g, '')
	.split(/\s*,\s*/);

clients?.forEach((client) => envClients.push(RedisClientPrefix[client]));

const REDIS_CLIENTS = envClients.length
	? envClients
	: [RedisClientPrefix.DLOB, RedisClientPrefix.DLOB_HELIUS];
console.log('Redis Clients:', REDIS_CLIENTS);

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
const serverPort = process.env.PORT || 6969;
export const ORDERBOOK_UPDATE_INTERVAL = 1000;
const WS_FALLBACK_FETCH_INTERVAL = ORDERBOOK_UPDATE_INTERVAL * 60;
const SLOT_STALENESS_TOLERANCE =
	parseInt(process.env.SLOT_STALENESS_TOLERANCE) || 1000;
const useWebsocket = process.env.USE_WEBSOCKET?.toLowerCase() === 'true';

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
app.use(handleResponseTime());

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
			openbook: undefined,
		};

		if (market.phoenixMarket) {
			const phoenixConfigAccount =
				await driftClient.getPhoenixV1FulfillmentConfig(market.phoenixMarket);
			if (isVariant(phoenixConfigAccount.status, 'enabled')) {
				const bulkAccountLoader = new BulkAccountLoader(
					driftClient.connection,
					stateCommitment,
					5_000
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

		if (market.openbookMarket) {
			const openbookConfigAccount =
				await driftClient.getOpenbookV2FulfillmentConfig(market.openbookMarket);
			if (isVariant(openbookConfigAccount.status, 'enabled')) {
				const openbookSubscriber = getOpenbookSubscriber(
					driftClient,
					market,
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

	const slotSubscriber = new SlotSubscriber(connection, {
		resubTimeoutMs: 5000,
	});
	await slotSubscriber.subscribe();

	driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription: {
			type: 'websocket',
			commitment: stateCommitment,
			resubTimeoutMs: 60_000,
		},
		env: driftEnv,
		delistedMarketSetting: DelistedMarketSetting.Discard,
	});

	let updatesReceivedTotal = 0;
	const orderSubscriber = new OrderSubscriber({
		driftClient,
		subscriptionConfig: {
			type: 'websocket',
			commitment: stateCommitment,
			resubTimeoutMs: 10_000,
			resyncIntervalMs: WS_FALLBACK_FETCH_INTERVAL,
		},
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
	logger.info('Connecting to redis');
	for (let i = 0; i < REDIS_CLIENTS.length; i++) {
		redisClients.push(new RedisClient({ prefix: REDIS_CLIENTS[i] }));
	}

	const fetchFromRedis = async (
		key: string,
		selectionCriteria: (responses: any) => any
	): Promise<any> => {
		const redisResponses = await Promise.all(
			redisClients.map((client) => client.getRaw(key))
		);
		return selectionCriteria(redisResponses);
	};

	const userMapClient = new RedisClient({
		host: process.env.ELASTICACHE_USERMAP_HOST ?? process.env.ELASTICACHE_HOST,
		port: process.env.ELASTICACHE_USERMAP_PORT ?? process.env.ELASTICACHE_PORT,
		prefix: RedisClientPrefix.USER_MAP,
	});

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

			const fees = await redisClients
				.find((client) => {
					return (
						client.forceGetClient().options.keyPrefix ===
						RedisClientPrefix.DLOB_HELIUS
					);
				})
				.getRaw(`priorityFees_${marketType}_${marketIndex}`);

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
					const fees = await redisClients
						.find(
							(client) =>
								client.forceGetClient().options.keyPrefix ===
								RedisClientPrefix.DLOB_HELIUS
						)
						.getRaw(
							`priorityFees_${normedParam['marketType']}_${normedParam['marketIndex']}`
						);

					return {
						...JSON.parse(fees),
						marketType: normedParam['marketType'],
						marketIndex: parseInt(normedParam['marketIndex']),
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
				includeAccounts,
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

			let accountFlag = false;
			if (includeAccounts) {
				accountFlag = (includeAccounts as string).toLowerCase() === 'true';
			}

			let topMakers: string[];

			const redisResponse = await fetchFromRedis(
				`last_update_orderbook_best_makers_${getVariant(
					normedMarketType
				)}_${marketIndex}`,
				selectMostRecentBySlot
			);
			if (redisResponse) {
				if (
					dlobProvider.getSlot() - redisResponse['slot'] <
					SLOT_STALENESS_TOLERANCE
				) {
					if (side === 'bid') {
						topMakers = redisResponse['bids'];
					} else {
						topMakers = redisResponse['asks'];
					}
				}
			}

			if (topMakers) {
				cacheHitCounter.add(1, {
					miss: false,
					path: req.baseUrl + req.path,
				});
				res.writeHead(200);

				if (accountFlag) {
					const topAccounts = await getRawAccountFromId(
						userMapClient,
						topMakers
					);
					res.end(JSON.stringify(topAccounts));
					return;
				}

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

			if (accountFlag) {
				const topAccounts = await getAccountFromId(userMapClient, topMakers);
				res.end(JSON.stringify(topAccounts));
				return;
			}

			res.end(JSON.stringify(topMakers));
		} catch (err) {
			next(err);
		}
	});

	// returns top 20 unsettled gainers and losers
	app.get('/unsettledPnlUsers', async (req, res, next) => {
		try {
			const marketIndex = Number(req.query.marketIndex as string);

			if (isNaN(marketIndex)) {
				res.status(400).send('Bad Request: must include a marketIndex');
				return;
			}

			const redisClient = redisClients.find(
				(client) =>
					client.forceGetClient().options.keyPrefix === RedisClientPrefix.DLOB
			);

			const redisResponseGainers = await redisClient.getRaw(
				`perp_market_${marketIndex}_gainers`
			);
			const redisResponseLosers = await redisClient.getRaw(
				`perp_market_${marketIndex}_losers`
			);

			const response = {
				marketIndex,
				gainers: JSON.parse(redisResponseGainers),
				losers: JSON.parse(redisResponseLosers),
			};

			res.end(JSON.stringify(response));
			return;
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
				includeOpenbook,
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
			if (!isSpot && `${includeVamm}`?.toLowerCase() === 'true') {
				const redisL2 = await fetchFromRedis(
					`last_update_orderbook_perp_${normedMarketIndex}`,
					selectMostRecentBySlot
				);
				const depth = Math.min(parseInt(adjustedDepth as string) ?? 1, 100);
				redisL2['bids'] = redisL2['bids']?.slice(0, depth);
				redisL2['asks'] = redisL2['asks']?.slice(0, depth);
				if (
					redisL2 &&
					dlobProvider.getSlot() - redisL2['slot'] < SLOT_STALENESS_TOLERANCE
				) {
					l2Formatted = JSON.stringify(redisL2);
				}
			} else if (
				isSpot &&
				`${includePhoenix}`?.toLowerCase() === 'true' &&
				`${includeOpenbook}`?.toLowerCase() === 'true'
			) {
				const redisL2 = await fetchFromRedis(
					`last_update_orderbook_spot_${normedMarketIndex}`,
					selectMostRecentBySlot
				);
				const depth = Math.min(parseInt(adjustedDepth as string) ?? 1, 100);
				redisL2['bids'] = redisL2['bids']?.slice(0, depth);
				redisL2['asks'] = redisL2['asks']?.slice(0, depth);
				if (
					redisL2 &&
					dlobProvider.getSlot() - redisL2['slot'] < SLOT_STALENESS_TOLERANCE
				) {
					l2Formatted = JSON.stringify(redisL2);
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

			let validateIncludeVamm = false;
			if (!isSpot && `${includeVamm}`.toLowerCase() === 'true') {
				const perpMarket = driftClient.getPerpMarketAccount(normedMarketIndex);
				validateIncludeVamm = !isOperationPaused(
					perpMarket.pausedOperations,
					PerpOperation.AMM_FILL
				);
			}
			const l2 = dlobSubscriber.getL2({
				marketIndex: normedMarketIndex,
				marketType: normedMarketType,
				depth: parseInt(adjustedDepth as string),
				includeVamm: validateIncludeVamm,
				numVammOrders: parseInt((numVammOrders ?? '100') as string),
				fallbackL2Generators: isSpot
					? [
							`${includePhoenix}`.toLowerCase() === 'true' &&
								MARKET_SUBSCRIBERS[normedMarketIndex].phoenix,
							`${includeOpenbook}`.toLowerCase() === 'true' &&
								MARKET_SUBSCRIBERS[normedMarketIndex].openbook,
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

	app.get(['/batchL2', '/batchL2Cache'], async (req, res, next) => {
		try {
			const {
				marketName,
				marketIndex,
				marketType,
				depth,
				includeVamm,
				includePhoenix,
				includeOpenbook,
				includeOracle,
			} = req.query;

			const normedParams = normalizeBatchQueryParams({
				marketName: marketName as string | undefined,
				marketIndex: marketIndex as string | undefined,
				marketType: marketType as string | undefined,
				depth: depth as string | undefined,
				includeVamm: includeVamm as string | undefined,
				includePhoenix: includePhoenix as string | undefined,
				includeOpenbook: includeOpenbook as string | undefined,
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
					if (!isSpot && normedParam['includeVamm']?.toLowerCase() === 'true') {
						const redisL2 = await fetchFromRedis(
							`last_update_orderbook_perp_${normedMarketIndex}`,
							selectMostRecentBySlot
						);
						const depth = Math.min(parseInt(adjustedDepth as string) ?? 1, 100);
						redisL2['bids'] = redisL2['bids']?.slice(0, depth);
						redisL2['asks'] = redisL2['asks']?.slice(0, depth);
						if (redisL2) {
							if (
								dlobProvider.getSlot() - redisL2['slot'] <
								SLOT_STALENESS_TOLERANCE
							) {
								l2Formatted = redisL2;
							}
						}
					} else if (
						isSpot &&
						normedParam['includePhoenix']?.toLowerCase() === 'true' &&
						normedParam['includeOpenbook']?.toLowerCase() === 'true'
					) {
						const redisL2 = await fetchFromRedis(
							`last_update_orderbook_spot_${normedMarketIndex}`,
							selectMostRecentBySlot
						);
						const depth = Math.min(parseInt(adjustedDepth as string) ?? 1, 100);
						redisL2['bids'] = redisL2['bids']?.slice(0, depth);
						redisL2['asks'] = redisL2['asks']?.slice(0, depth);
						if (redisL2) {
							if (
								dlobProvider.getSlot() - redisL2['slot'] <
								SLOT_STALENESS_TOLERANCE
							) {
								l2Formatted = redisL2;
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

					let validateIncludeVamm = false;
					if (!isSpot && `${includeVamm}`.toLowerCase() === 'true') {
						const perpMarket =
							driftClient.getPerpMarketAccount(normedMarketIndex);
						validateIncludeVamm = !isOperationPaused(
							perpMarket.pausedOperations,
							PerpOperation.AMM_FILL
						);
					}
					const l2 = dlobSubscriber.getL2({
						marketIndex: normedMarketIndex,
						marketType: normedMarketType,
						depth: parseInt(adjustedDepth as string),
						includeVamm: validateIncludeVamm,
						fallbackL2Generators: isSpot
							? [
									`${normedParam['includePhoenix']}`.toLowerCase() === 'true' &&
										MARKET_SUBSCRIBERS[normedMarketIndex].phoenix,
									`${normedParam['includeOpenbook']}`.toLowerCase() ===
										'true' && MARKET_SUBSCRIBERS[normedMarketIndex].openbook,
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

			const redisL3 = await fetchFromRedis(
				`last_update_orderbook_l3_${marketTypeStr}_${normedMarketIndex}`,
				selectMostRecentBySlot
			);
			if (
				redisL3 &&
				dlobProvider.getSlot() - redisL3['slot'] < SLOT_STALENESS_TOLERANCE
			) {
				cacheHitCounter.add(1, {
					miss: false,
					path: req.baseUrl + req.path,
				});
				res.writeHead(200);
				res.end(JSON.stringify(redisL3));
				return;
			} else {
				cacheHitCounter.add(1, {
					miss: true,
					path: req.baseUrl + req.path,
				});
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
