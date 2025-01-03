import { program } from 'commander';
import compression from 'compression';
import cors from 'cors';
import express from 'express';
import morgan from 'morgan';

import { Commitment, Connection } from '@solana/web3.js';

import {
	DriftEnv,
	SlotSubscriber,
	initialize,
	MarketType,
	getVariant,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';

import { logger, setLogLevel } from './utils/logger';

import * as http from 'http';
import {
	handleHealthCheck,
	cacheHitCounter,
	incomingRequestsCounter,
	runtimeSpecsGauge,
} from './core/metrics';
import { handleResponseTime } from './core/middleware';
import { errorHandler, selectMostRecentBySlot, sleep } from './utils/utils';
import { setGlobalDispatcher, Agent } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();

// Reading in Redis env vars
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
	parseInt(process.env.SLOT_STALENESS_TOLERANCE) || 100000;

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
logger.info(`DriftEnv:           ${driftEnv}`);
logger.info(`Commit:             ${commitHash}`);

const main = async (): Promise<void> => {
	const connection = new Connection(endpoint, {
		wsEndpoint,
		commitment: stateCommitment,
	});

	const slotSubscriber = new SlotSubscriber(connection, {
		resubTimeoutMs: 5000,
	});
	await slotSubscriber.subscribe();

	// Handle redis client initialization and rotation maps
	const redisClients: Array<RedisClient> = [];
	logger.info('Connecting to redis');
	for (let i = 0; i < REDIS_CLIENTS.length; i++) {
		redisClients.push(new RedisClient({ prefix: REDIS_CLIENTS[i] }));
	}

	const fetchFromRedis = async (
		key: string,
		selectionCriteria: (responses: any) => any
	): Promise<JSON> => {
		const redisResponses = await Promise.all(
			redisClients.map((client) => client.getRaw(key))
		);
		return selectionCriteria(redisResponses);
	};

	const handleStartup = async (_req, res, _next) => {
		if (slotSubscriber.currentSlot && !redisClients.some((c) => !c.connected)) {
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(500);
			res.end('Not ready');
		}
	};

	app.get(
		'/health',
		handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, slotSubscriber)
	);
	app.get('/startup', handleStartup);
	app.get(
		'/',
		handleHealthCheck(2 * WS_FALLBACK_FETCH_INTERVAL, slotSubscriber)
	);

	app.get('/l3', async (req, res, next) => {
		try {
			const { marketIndex, marketType } = req.query;

			const isSpot = (marketType as string).toLowerCase() === 'spot';
			const normedMarketIndex = parseInt(marketIndex as string);
			const normedMarketType = isSpot ? MarketType.SPOT : MarketType.PERP;

			const redisL3 = await fetchFromRedis(
				`last_update_orderbook_l3_${getVariant(
					normedMarketType
				)}_${normedMarketIndex}`,
				selectMostRecentBySlot
			);
			if (
				redisL3 &&
				slotSubscriber.getSlot() - redisL3['slot'] < SLOT_STALENESS_TOLERANCE
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
				res.writeHead(500);
				res.end(JSON.stringify({ error: 'No cached L3 found' }));
			}
		} catch (err) {
			next(err);
		}
	});

	server.listen(serverPort, () => {
		logger.info(
			`DLOB server lite listening on port http://localhost:${serverPort}`
		);
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

export { commitHash, driftEnv, endpoint, sdkConfig, wsEndpoint };
