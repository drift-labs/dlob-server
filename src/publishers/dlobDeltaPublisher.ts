import {
	DelistedMarketSetting,
	DLOBSubscriber,
	DriftClient,
	DriftEnv,
	OrderSubscriber,
	Wallet,
} from '@drift-labs/sdk';
import { Commitment, Connection, Keypair } from '@solana/web3.js';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';
import { logger } from '@drift/common';
import express from 'express';

import { OrderbookDeltaTracker } from '../services/orderbookDeltaTracker';
import { getDLOBProviderFromOrderSubscriber } from '../dlobProvider';
import { l2WithBNToStrings, sleep } from '../utils/utils';
import { handleHealthCheck } from '../core/metrics';

const ENDPOINT = process.env.ENDPOINT;
const URL = process.env.URL ?? ENDPOINT.slice(0, ENDPOINT.lastIndexOf('/'));
const TOKEN =
	process.env.TOKEN ?? ENDPOINT.slice(ENDPOINT.lastIndexOf('/') + 1);
const REDIS_CLIENT = process.env.REDIS_CLIENT || 'DLOB';

const connection = new Connection(ENDPOINT, 'confirmed');
const wallet = new Wallet(new Keypair());
const stateCommitment: Commitment = 'confirmed';

const driftClient = new DriftClient({
	env: (process.env.ENV ?? 'mainnet-beta') as DriftEnv,
	connection,
	wallet,
	delistedMarketSetting: DelistedMarketSetting.Discard,
});

const redisClient = new RedisClient({
	prefix: RedisClientPrefix[REDIS_CLIENT],
});

const indicativeRedisClient = new RedisClient({});

const app = express();

async function main() {
	await redisClient.connect();
	await indicativeRedisClient.connect();
	await driftClient.subscribe();

	const perpMarkets = await driftClient.getPerpMarketAccounts();

	const { processOrderbook, addIndicativeLiquidity } = OrderbookDeltaTracker(
		redisClient,
		indicativeRedisClient
	);

	const orderSubscriber = new OrderSubscriber({
		driftClient,
		subscriptionConfig: {
			type: 'grpc',
			grpcConfigs: {
				endpoint: URL,
				token: TOKEN,
				// @ts-ignore
				commitmentLevel: 'confirmed',
				channelOptions: {
					'grpc.keepalive_time_ms': 10_000,
					'grpc.keepalive_timeout_ms': 1_000,
					'grpc.keepalive_permit_without_calls': 1,
				},
			},
			commitment: stateCommitment,
		},
	});

	const slotSource = {
		getSlot: () => orderSubscriber.getSlot(),
	};

	const dlobProvider = getDLOBProviderFromOrderSubscriber(orderSubscriber);
	await dlobProvider.subscribe();

	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: dlobProvider,
		slotSource,
		updateFrequency: 1000,
		protectedMakerView: true,
	});

	await dlobSubscriber.subscribe();

	const lastProcessedSlot = new Map<number, number>();

	setInterval(() => {
		perpMarkets.map(async (market) => {
			const l2 = dlobSubscriber.getL2({
				marketIndex: market.marketIndex,
				marketType: { perp: {} },
				depth: -1,
				includeVamm: true,
				numVammOrders: 100,
			});

			const l2Formatted = l2WithBNToStrings(l2);
			const currentSlot = l2Formatted.slot;
			const lastSlot = lastProcessedSlot.get(market.marketIndex) || 0;

			if (currentSlot > lastSlot) {
				processOrderbook({
					...l2Formatted,
					marketIndex: market.marketIndex,
				});
				lastProcessedSlot.set(market.marketIndex, currentSlot);
			}
		});
	}, 200);

	// Run the liquidity addition only every 1000ms (1 second)
	setInterval(() => {
		perpMarkets.map(async (market) => {
			await addIndicativeLiquidity(dlobSubscriber, market.marketIndex);
		});
	}, 1000);

	const handleStartup = async (_req, res, _next) => {
		if (driftClient.isSubscribed && dlobProvider.size() > 0) {
			res.writeHead(200);
			res.end('OK');
		} else {
			res.writeHead(500);
			res.end('Not ready');
		}
	};

	app.get('/startup', handleStartup);
	app.get('/health', handleHealthCheck(60_000, dlobProvider));
	app.listen(8080);
	logger.info('Orderbook delta tracker started successfully');
}

async function recursiveTryCatch(f: () => void) {
	try {
		await f();

		process.on('uncaughtException', async (err) => {
			logger.error(`Uncaught exception: ${err}`);
			await sleep(15000);
			await recursiveTryCatch(f);
		});

		process.on('unhandledRejection', async (reason) => {
			console.log('rejection');
			logger.error(`Unhandled rejection: ${reason}`);
			await sleep(15000);
			await recursiveTryCatch(f);
		});
	} catch (e) {
		logger.error(e);
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => main());
