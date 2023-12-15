import { program } from 'commander';
import compression from 'compression';
import cors from 'cors';
import express from 'express';
import rateLimit from 'express-rate-limit';
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
import { RedisClient } from './utils/redisClient';
import {
	DriftEnv,
	MarketType,
	PerpMarkets,
	SlotSubscriber,
	SpotMarkets,
	isVariant,
	initialize,
} from '@drift-labs/sdk';

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
const rateLimitCallsPerSecond = process.env.RATE_LIMIT_CALLS_PER_SECOND
	? parseInt(process.env.RATE_LIMIT_CALLS_PER_SECOND)
	: 1;

const SLOT_STALENESS_TOLERANCE =
	parseInt(process.env.SLOT_STALENESS_TOLERANCE) || 20;
const loadTestAllowed = process.env.ALLOW_LOAD_TEST?.toLowerCase() === 'true';

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
	logger.info('Connecting to redis');
	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	await redisClient.connect();

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
				`${includeVamm}`.toLowerCase() === 'true' &&
				`${includeOracle}`.toLowerCase().toLowerCase() === 'true' &&
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
				if (redisL2) {
					slotDiff =
						slotSubscriber.getSlot() - parseInt(JSON.parse(redisL2).slot);
					if (slotDiff < SLOT_STALENESS_TOLERANCE) {
						l2Formatted = redisL2;
					}
				}
			} else if (
				isSpot &&
				`${includeSerum}`.toLowerCase() === 'true' &&
				`${includePhoenix}`.toLowerCase() === 'true' &&
				`${includeOracle}`.toLowerCase() === 'true' &&
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
				if (redisL2) {
					slotDiff =
						slotSubscriber.getSlot() - parseInt(JSON.parse(redisL2).slot);
					if (slotDiff < SLOT_STALENESS_TOLERANCE) {
						l2Formatted = redisL2;
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
						normedParam['includeVamm'].toLowerCase() === 'true' &&
						normedParam['includeOracle'].toLowerCase() === 'true' &&
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
							if (
								slotSubscriber.getSlot() - parseInt(parsedRedisL2.slot) <
								SLOT_STALENESS_TOLERANCE
							)
								l2Formatted = parsedRedisL2;
						}
					} else if (
						isSpot &&
						normedParam['includePhoenix'].toLowerCase() === 'true' &&
						normedParam['includeSerum'].toLowerCase() === 'true' &&
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
							if (
								slotSubscriber.getSlot() - parseInt(parsedRedisL2.slot) <
								SLOT_STALENESS_TOLERANCE
							)
								l2Formatted = parsedRedisL2;
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
