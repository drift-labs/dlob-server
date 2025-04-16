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
	errorHandler,
	normalizeBatchQueryParams,
	sleep,
	validateDlobQuery,
	getAccountFromId,
	getRawAccountFromId,
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
				if (side === 'bid') {
					topMakers = redisResponse['bids'];
				} else {
					topMakers = redisResponse['asks'];
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
						topMakers,
						driftClient.connection
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
			const { marketName, marketIndex, marketType, depth, includeIndicative } =
				req.query;

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
			const includeIndicativeStr =
				(includeIndicative as string)?.toLowerCase() === 'true';
			const adjustedDepth = depth ?? '100';

			let l2Formatted: any;
			const redisL2 = await fetchFromRedis(
				`last_update_orderbook_${
					isSpot ? 'spot' : 'perp'
				}_${normedMarketIndex}${includeIndicativeStr ? '_indicative' : ''}`,
				selectMostRecentBySlot
			);
			console.log(
				`last_update_orderbook_${
					isSpot ? 'spot' : 'perp'
				}_${normedMarketIndex}${includeIndicativeStr ? '_indicative' : ''}`
			);
			const depthToUse = Math.min(parseInt(adjustedDepth as string) ?? 1, 100);
			let cacheMiss = true;
			if (redisL2) {
				cacheMiss = false;
				redisL2['bids'] = redisL2['bids']?.slice(0, depthToUse);
				redisL2['asks'] = redisL2['asks']?.slice(0, depthToUse);
				l2Formatted = redisL2;
			} else {
				console.log(
					`No L2 found for ${getVariant(
						normedMarketType
					)} market ${normedMarketIndex}`
				);
				const oracleData = isSpot
					? driftClient.getOracleDataForSpotMarket(normedMarketIndex)
					: driftClient.getOracleDataForPerpMarket(normedMarketIndex);
				l2Formatted = {
					bids: [],
					asks: [],
					marketType: normedMarketType,
					marketIndex: normedMarketIndex,
					marketName: undefined,
					slot: dlobProvider.getSlot(),
					oracle: oracleData.price.toNumber(),
					oracleData: {
						price: oracleData.price.toNumber(),
						slot: oracleData.slot.toNumber(),
						confidence: oracleData.confidence.toNumber(),
						hasSufficientNumberOfDataPoints: true,
						twap: oracleData.twap.toNumber(),
						twapConfidence: oracleData.twapConfidence.toNumber(),
					},
					ts: Date.now(),
					marketSlot: dlobProvider.getSlot(),
				};
			}
			cacheHitCounter.add(1, {
				miss: cacheMiss,
				path: req.baseUrl + req.path,
			});
			res.writeHead(200);
			res.end(JSON.stringify(l2Formatted));
			return;
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
				includeIndicative,
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
				includeIndicative: includeIndicative as string | undefined,
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
					const normedIncludeIndicative =
						normedParam['includeIndicative'] == 'true';

					const adjustedDepth = normedParam['depth'] ?? '100';
					let l2Formatted: any;
					const redisL2 = await fetchFromRedis(
						`last_update_orderbook_${
							isSpot ? 'spot' : 'perp'
						}_${normedMarketIndex}${
							normedIncludeIndicative ? '_indicative' : ''
						}`,
						selectMostRecentBySlot
					);
					const depth = Math.min(parseInt(adjustedDepth as string) ?? 1, 100);
					let cacheMiss = true;
					if (redisL2) {
						cacheMiss = false;
						redisL2['bids'] = redisL2['bids']?.slice(0, depth);
						redisL2['asks'] = redisL2['asks']?.slice(0, depth);
						l2Formatted = redisL2;
					} else {
						console.log(
							`No L2 found for ${getVariant(
								normedMarketType
							)} market ${normedMarketIndex}`
						);
						const oracleData = isSpot
							? driftClient.getOracleDataForSpotMarket(normedMarketIndex)
							: driftClient.getOracleDataForPerpMarket(normedMarketIndex);
						l2Formatted = {
							bids: [],
							asks: [],
							marketType: normedMarketType,
							marketIndex: normedMarketIndex,
							marketName: undefined,
							slot: dlobProvider.getSlot(),
							oracle: oracleData.price.toNumber(),
							oracleData: {
								price: oracleData.price.toNumber(),
								slot: oracleData.slot.toNumber(),
								confidence: oracleData.confidence.toNumber(),
								hasSufficientNumberOfDataPoints: true,
								twap: oracleData.twap.toNumber(),
								twapConfidence: oracleData.twapConfidence.toNumber(),
							},
							ts: Date.now(),
							marketSlot: dlobProvider.getSlot(),
						};
					}

					if (l2Formatted) {
						cacheHitCounter.add(1, {
							miss: cacheMiss,
							path: req.baseUrl + req.path,
						});
					} else {
						cacheHitCounter.add(1, {
							miss: cacheMiss,
							path: req.baseUrl + req.path,
						});
					}
					return l2Formatted;
				})
			);

			if (hasError) {
				res.status(400).send(errorMessage);
				return;
			}

			res.writeHead(200);
			res.end(JSON.stringify({ l2s }));
			return;
		} catch (err) {
			next(err);
		}
	});

	app.get('/l3', async (req, res, next) => {
		try {
			const { marketName, marketIndex, marketType, includeIndicative } =
				req.query;

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
			const normedIncludeIndicative =
				(includeIndicative as string)?.toLowerCase() === 'true';

			const redisL3 = await fetchFromRedis(
				`last_update_orderbook_l3_${marketTypeStr}_${normedMarketIndex}${
					normedIncludeIndicative ? '_indicative' : ''
				}`,
				selectMostRecentBySlot
			);
			if (redisL3) {
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
				res.writeHead(500);
				res.end('No L3 found');
				return;
			}
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
