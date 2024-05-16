import { program } from 'commander';
import compression from 'compression';
import cors from 'cors';
import express from 'express';
import morgan from 'morgan';

import { Commitment, Connection } from '@solana/web3.js';

import { logger, setLogLevel } from './utils/logger';

import * as http from 'http';
import {
	handleHealthCheck,
	cacheHitCounter,
	runtimeSpecsGauge,
} from './core/metrics';
import { handleResponseTime } from './core/middleware';
import { errorHandler, normalizeBatchQueryParams, sleep } from './utils/utils';
import {
	DriftEnv,
	MarketType,
	PerpMarkets,
	SlotSubscriber,
	SpotMarkets,
	isVariant,
	initialize,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common';

require('dotenv').config();

// Reading in Redis env vars
const REDIS_CLIENTS = process.env.REDIS_CLIENTS?.replace(/^\[|\]$/g, '')
	.split(',')
	.map((clients) => clients.trim()) || ['DLOB'];

console.log('Redis Clients:', REDIS_CLIENTS);

const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'processed';
const serverPort = process.env.PORT || 6969;

const SLOT_STALENESS_TOLERANCE =
	parseInt(process.env.SLOT_STALENESS_TOLERANCE) || 20;

const logFormat =
	':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length] ":referrer" ":user-agent" :req[x-forwarded-for]';
const logHttp = morgan(logFormat, {
	skip: (_req, res) => res.statusCode <= 500,
});

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);
app.use(logHttp);
app.use(handleResponseTime);

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

// strip off /ui, if the request comes from the UI
app.use((req, _res, next) => {
	if (req.url.startsWith('/ui')) {
		req.url = req.url.replace('/ui', '');
		if (req.url === '') {
			req.url = '/';
		}
	}
	next();
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
logger.info(`DriftEnv:           ${driftEnv}`);
logger.info(`Commit:             ${commitHash}`);

const main = async () => {
	// Redis connect
	const redisClients: Array<RedisClient> = [];
	const spotMarketRedisMap: Map<
		number,
		{ client: RedisClient; clientIndex: number }
	> = new Map();
	const perpMarketRedisMap: Map<
		number,
		{ client: RedisClient; clientIndex: number }
	> = new Map();
	for (let i = 0; i < REDIS_CLIENTS.length; i++) {
		const prefix = RedisClientPrefix[REDIS_CLIENTS[i]];
		redisClients.push(new RedisClient({ prefix }));
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

	// Slot subscriber for source
	const connection = new Connection(endpoint, {
		commitment: stateCommitment,
		wsEndpoint,
	});
	const slotSubscriber = new SlotSubscriber(connection);
	await slotSubscriber.subscribe();

	app.get(
		'/health',
		handleHealthCheck(2 * SLOT_STALENESS_TOLERANCE * 400, slotSubscriber)
	);
	app.get(
		'/',
		handleHealthCheck(2 * SLOT_STALENESS_TOLERANCE * 400, slotSubscriber)
	);

	const handleStartup = async (_req, res, _next) => {
		let healthy = false;
		for (const redisClient of redisClients) {
			if (redisClient.connected) {
				healthy = true;
			}
		}
		if (healthy) {
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(500);
			res.end('Not ready');
		}
	};
	app.get('/startup', handleStartup);

	app.get('/l2', async (req, res, next) => {
		try {
			const {
				marketIndex,
				marketType,
				depth,
				includeVamm,
				includePhoenix,
				includeSerum,
				grouping, // undefined or PRICE_PRECISION
				includeOracle,
			} = req.query;

			const { normedMarketType, normedMarketIndex, error } = validateDlobQuery(
				driftEnv,
				marketType as string,
				marketIndex as string
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
			let slotDiff: number;
			if (
				!isSpot &&
				`${includeVamm}`?.toLowerCase() === 'true' &&
				`${includeOracle}`?.toLowerCase().toLowerCase() === 'true' &&
				!grouping
			) {
				let redisL2: string;
				const redisClient = perpMarketRedisMap.get(normedMarketIndex).client;
				if (parseInt(adjustedDepth as string) === 5) {
					redisL2 = await redisClient.getRaw(
						`last_update_orderbook_perp_${normedMarketIndex}_depth_5`
					);
				} else if (parseInt(adjustedDepth as string) === 20) {
					redisL2 = await redisClient.getRaw(
						`last_update_orderbook_perp_${normedMarketIndex}_depth_20`
					);
				} else if (parseInt(adjustedDepth as string) === 100) {
					redisL2 = await redisClient.getRaw(
						`last_update_orderbook_perp_${normedMarketIndex}_depth_100`
					);
				}
				if (redisL2) {
					slotDiff =
						slotSubscriber.getSlot() - parseInt(JSON.parse(redisL2).slot);
					if (slotDiff < SLOT_STALENESS_TOLERANCE) {
						l2Formatted = redisL2;
					} else {
						if (redisClients.length > 1) {
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
					redisL2 = await redisClient.getRaw(
						`last_update_orderbook_spot_${normedMarketIndex}_depth_5`
					);
				} else if (parseInt(adjustedDepth as string) === 20) {
					redisL2 = await redisClient.getRaw(
						`last_update_orderbook_spot_${normedMarketIndex}_depth_20`
					);
				} else if (parseInt(adjustedDepth as string) === 100) {
					redisL2 = await redisClient.getRaw(
						`last_update_orderbook_spot_${normedMarketIndex}_depth_100`
					);
				}
				if (redisL2) {
					slotDiff =
						slotSubscriber.getSlot() - parseInt(JSON.parse(redisL2).slot);
					if (slotDiff < SLOT_STALENESS_TOLERANCE) {
						l2Formatted = redisL2;
					} else {
						if (redisClients.length > 1) {
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
			}

			if (l2Formatted) {
				cacheHitCounter.add(1, {
					miss: false,
				});
				res.writeHead(200);
				res.end(l2Formatted);
				return;
			} else {
				if (slotDiff) {
					res.writeHead(500);
					res.end(`Slot too stale : ${slotDiff}`);
				} else {
					res.writeHead(400);
					res.end(`Bad Request: no cached L2 found`);
				}
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

			const l2s = await Promise.all(
				normedParams.map(async (normedParam) => {
					const { normedMarketType, normedMarketIndex, error } =
						validateDlobQuery(
							driftEnv,
							normedParam['marketType'] as string,
							normedParam['marketIndex'] as string
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
							redisL2 = await redisClient.getRaw(
								`last_update_orderbook_perp_${normedMarketIndex}_depth_5`
							);
						} else if (parseInt(adjustedDepth as string) === 20) {
							redisL2 = await redisClient.getRaw(
								`last_update_orderbook_perp_${normedMarketIndex}_depth_20`
							);
						} else if (parseInt(adjustedDepth as string) === 100) {
							redisL2 = await redisClient.getRaw(
								`last_update_orderbook_perp_${normedMarketIndex}_depth_100`
							);
						}
						if (redisL2) {
							const parsedRedisL2 = JSON.parse(redisL2);
							if (
								slotSubscriber.getSlot() - parseInt(parsedRedisL2.slot) <
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
							redisL2 = await redisClient.getRaw(
								`last_update_orderbook_spot_${normedMarketIndex}_depth_5`
							);
						} else if (parseInt(adjustedDepth as string) === 20) {
							redisL2 = await redisClient.getRaw(
								`last_update_orderbook_spot_${normedMarketIndex}_depth_20`
							);
						} else if (parseInt(adjustedDepth as string) === 100) {
							redisL2 = await redisClient.getRaw(
								`last_update_orderbook_spot_${normedMarketIndex}_depth_100`
							);
						}
						if (redisL2) {
							const parsedRedisL2 = JSON.parse(redisL2);
							if (
								slotSubscriber.getSlot() - parseInt(parsedRedisL2.slot) <
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

						if (l2Formatted) {
							cacheHitCounter.add(1, {
								miss: false,
							});
							return l2Formatted;
						}
					}
				})
			);

			res.writeHead(200);
			res.end(JSON.stringify({ l2s }));
		} catch (err) {
			next(err);
		}
	});

	server.listen(serverPort, () => {
		logger.info(`DLOB server listening on port http://localhost:${serverPort}`);
	});
};

export const validateDlobQuery = (
	driftEnv: DriftEnv,
	marketType?: string,
	marketIndex?: string
): {
	normedMarketType?: MarketType;
	normedMarketIndex?: number;
	error?: string;
} => {
	let normedMarketType: MarketType = undefined;
	let normedMarketIndex: number = undefined;
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

	return {
		normedMarketType,
		normedMarketIndex,
	};
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

export { commitHash, endpoint, sdkConfig, wsEndpoint };
