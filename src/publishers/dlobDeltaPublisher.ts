import {
	DelistedMarketSetting,
	DLOBSubscriber,
	DriftClient,
	DriftEnv,
	MarketType,
	OrderSubscriber,
	Wallet,
} from '@drift-labs/sdk';
import { Commitment, Connection, Keypair } from '@solana/web3.js';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';
import { logger } from '@drift/common';
import express from 'express';

import { OrderbookDeltaTracker } from '../services/orderbookDeltaTracker';
import { getDLOBProviderFromOrderSubscriber } from '../dlobProvider';
import {
	addOracletoResponse,
	initializeAllMarketSubscribers,
	l2WithBNToStrings,
} from '../utils/utils';
import { handleHealthCheck } from '../core/metrics';

const ENDPOINT = process.env.ENDPOINT;
const URL = process.env.URL ?? ENDPOINT.slice(0, ENDPOINT.lastIndexOf('/'));
const TOKEN =
	process.env.TOKEN ?? ENDPOINT.slice(ENDPOINT.lastIndexOf('/') + 1);
const REDIS_CLIENT = process.env.REDIS_CLIENT || 'DLOB';
const MARKET_TYPE = process.env.MARKET_TYPE ?? 'perp';

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

	const marketType = MARKET_TYPE === 'perp' ? MarketType.PERP : MarketType.SPOT;

	const markets =
		marketType === MarketType.PERP
			? await driftClient.getPerpMarketAccounts()
			: await driftClient.getSpotMarketAccounts();

	let MARKET_SUBSCRIBERS = {};
	if (marketType === MarketType.SPOT) {
		MARKET_SUBSCRIBERS = await initializeAllMarketSubscribers(driftClient);
	}

	const { processOrderbook, addIndicativeLiquidity } = OrderbookDeltaTracker(
		redisClient,
		indicativeRedisClient,
		marketType
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
		updateFrequency: 1000, // Doesn't matter since we are not subscribing and will be handled before adding indicative liquidity
		protectedMakerView: true,
	});

	const lastProcessedSlot = new Map<number, number>();

	const processMarkets = async () => {
		await dlobSubscriber.updateDLOB();

		await Promise.all(
			markets
			//@ts-ignore
				.filter(market => market.marketIndex === 0)
				.map(async (market) => {
					console.time('start')
					const marketIndex = market.marketIndex;
					await addIndicativeLiquidity(dlobSubscriber, marketIndex);

					const l2 = dlobSubscriber.getL2({
						marketIndex,
						marketType,
						depth: -1,
						includeVamm: true,
						numVammOrders: 100,
						fallbackL2Generators:
							marketType === MarketType.SPOT && MARKET_SUBSCRIBERS[marketIndex]
								? [
										MARKET_SUBSCRIBERS[marketIndex].phoenix,
										MARKET_SUBSCRIBERS[marketIndex].openbook,
								  ].filter((generator) => !!generator)
								: undefined,
					});

					const l2Formatted = l2WithBNToStrings(l2);
					const currentSlot = l2Formatted.slot;
					const lastSlot = lastProcessedSlot.get(marketIndex) || 0;

					addOracletoResponse(
						l2Formatted,
						driftClient,
						marketType,
						market.marketIndex
					);

					if (currentSlot > lastSlot) {
						await processOrderbook({
							...l2Formatted,
							marketIndex,
						});
						lastProcessedSlot.set(marketIndex, currentSlot);
					}
					console.timeEnd('start')
				})
		);
	};

	const scheduleNextRun = () => {
		setTimeout(async () => {
			await processMarkets();
			scheduleNextRun();
		}, 100);
	};

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

	// Start the processing loop
	scheduleNextRun();

	logger.info(
		`${
			marketType === MarketType.PERP ? 'Perp' : 'Spot'
		} market orderbook delta tracker started successfully`
	);
}

main().catch(async (error) => {
	const { message } = error;
	await logger.error(`Unhandled error in main: ${message}`);
	throw error;
});
