import { program } from 'commander';

import { Connection, Commitment, PublicKey, Keypair } from '@solana/web3.js';

import {
	DriftClient,
	initialize,
	DriftEnv,
	SlotSubscriber,
	Wallet,
	EventSubscriber,
	OrderAction,
	convertToNumber,
	BASE_PRECISION,
	QUOTE_PRECISION,
	PRICE_PRECISION,
	getVariant,
	ZERO,
	BN,
	OrderActionRecord,
	Event,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift-labs/common/clients';
import express from 'express';

import { Metrics } from '../core/metricsV2';
import { logger, setLogLevel } from '../utils/logger';
import { sleep } from '../utils/utils';
import {
	CompetitiveLiquidity,
	getAbsoluteBpsDiff,
	getCompetitiveLiquidity,
	getFillPrice,
	getFillSide,
	getFillTimestampMs,
	getIndicativeBpsBucket,
	getMakerMetricAttrs,
	getQuoteTimestampMs,
	getQuoteValueOnBook,
	IndicativeQuoteBlob,
} from './tradeMetrics';
import { fromEvent, filter, map } from 'rxjs';
import { setGlobalDispatcher, Agent } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
const REDIS_CLIENT = process.env.REDIS_CLIENT || 'DLOB';
const metricsPort = process.env.METRICS_PORT
	? parseInt(process.env.METRICS_PORT)
	: 9464;
const indicativeQuoteMaxAgeMs = process.env.INDICATIVE_QUOTES_MAX_AGE_MS
	? parseInt(process.env.INDICATIVE_QUOTES_MAX_AGE_MS)
	: 1000;
console.log('Redis Clients:', REDIS_CLIENT);
const redisClientPrefix = RedisClientPrefix[REDIS_CLIENT];
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
let driftClient: DriftClient;

type FillEvent = {
	ts: number;
	marketIndex: number;
	marketType: string;
	filler?: string;
	takerFee: number;
	makerFee: number;
	quoteAssetAmountSurplus: number;
	baseAssetAmountFilled: number;
	quoteAssetAmountFilled: number;
	taker?: string;
	takerOrderId?: number;
	takerOrderDirection?: string;
	takerOrderBaseAssetAmount: number;
	takerOrderCumulativeBaseAssetAmountFilled: number;
	takerOrderCumulativeQuoteAssetAmountFilled: number;
	maker?: string;
	makerOrderId?: number;
	makerOrderDirection?: string;
	makerOrderBaseAssetAmount: number;
	makerOrderCumulativeBaseAssetAmountFilled: number;
	makerOrderCumulativeQuoteAssetAmountFilled: number;
	oraclePrice: number;
	txSig: string;
	slot: number;
	fillRecordId?: number;
	action: string;
	actionExplanation: string;
	referrerReward: number;
	bitFlags: number;
};

type MarketQuoteCacheEntry = {
	fetchedAtMs: number;
	quoteBlobs: Array<{
		maker: string;
		quoteBlob: IndicativeQuoteBlob | null;
	}>;
};

type MarketQuoteEvaluation = {
	maker: string;
	totalQuoteValueOnBook: number;
	competitiveQuoteValueOnBook: number;
	competitiveLiquidity?: CompetitiveLiquidity;
};

const metricsV2 = new Metrics('trades-publisher', undefined, metricsPort);
const marketFillCount = metricsV2.addCounter(
	'market_fill_count',
	'Total market fills considered for JIT competitive opportunity metrics'
);
const indicativePresenceCount = metricsV2.addCounter(
	'indicative_presence_total',
	'Count of fills where a maker had any fresh indicative quote on the relevant side'
);
const indicativeCompetitiveOpportunityCount = metricsV2.addCounter(
	'indicative_competitive_opportunity_total',
	'Count of market fills where a maker had a fresh competitive indicative quote'
);
const indicativeCompetitiveFillCount = metricsV2.addCounter(
	'indicative_competitive_fill_total',
	'Count of competitive opportunities where the maker captured the fill'
);
const indicativeCompetitiveOpportunityNotional = metricsV2.addCounter(
	'indicative_competitive_opportunity_notional_total',
	'Total competitive opportunity notional in quote units for each maker'
);
const indicativeCompetitiveCapturedNotional = metricsV2.addCounter(
	'indicative_competitive_captured_notional_total',
	'Total captured notional in quote units on competitive opportunities for each maker'
);
const indicativeFillVsQuoteBucketCount = metricsV2.addCounter(
	'indicative_fill_vs_quote_bucket_total',
	'Count of maker fills bucketed by absolute bps distance from the best competitive indicative quote'
);
const indicativeQuoteEvaluationCount = metricsV2.addCounter(
	'indicative_quote_evaluation_total',
	'Count of quote evaluation outcomes by maker and market'
);
const indicativeTotalSizeOnBookGauge = metricsV2.addGauge(
	'indicative_total_size_on_book',
	'Latest fresh total quoted value on book by maker, market, and side'
);
const indicativeCompetitiveSizeOnBookGauge = metricsV2.addGauge(
	'indicative_competitive_size_on_book',
	'Latest fresh competitive quoted value on book by maker, market, and side'
);
metricsV2.finalizeObservables();

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const endpoint = process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
const indicativeQuotesCacheTtlMs = process.env.INDICATIVE_QUOTES_CACHE_TTL_MS
	? parseInt(process.env.INDICATIVE_QUOTES_CACHE_TTL_MS)
	: 250;
const enableMockFillEndpoint =
	process.env.ENABLE_MOCK_FILL_ENDPOINT?.toLowerCase() === 'true';
const mockOnlyMode = process.env.MOCK_ONLY_MODE?.toLowerCase() === 'true';
const mockFillPort = process.env.MOCK_FILL_PORT
	? parseInt(process.env.MOCK_FILL_PORT)
	: 9470;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

const marketQuoteCache = new Map<string, MarketQuoteCacheEntry>();

/**
 * Loads the current market maker quote blobs for a market/side and caches them briefly to reduce Redis load.
 */
const getMarketQuotes = async (
	indicativeQuotesRedisClient: RedisClient,
	fillEvent: FillEvent,
	side: 'long' | 'short',
	fillPrice: number,
	fillTsMs: number
): Promise<MarketQuoteEvaluation[]> => {
	const marketKey = `${fillEvent.marketType}_${fillEvent.marketIndex}_${side}`;
	const cached = marketQuoteCache.get(marketKey);
	if (cached && Date.now() - cached.fetchedAtMs <= indicativeQuotesCacheTtlMs) {
		return cached.quoteBlobs.map(({ maker, quoteBlob }) => {
			const spotPrecision =
				fillEvent.marketType === 'spot'
					? sdkConfig.SPOT_MARKETS[fillEvent.marketIndex].precision
					: undefined;
			const competitiveLiquidity = getCompetitiveLiquidity(
				maker,
				fillEvent,
				side,
				fillPrice,
				quoteBlob,
				spotPrecision
			);
			const quoteTsMs = getQuoteTimestampMs(quoteBlob);
			const quoteAgeMs =
				quoteTsMs !== undefined ? fillTsMs - quoteTsMs : Infinity;
			const isFresh = quoteAgeMs >= 0 && quoteAgeMs <= indicativeQuoteMaxAgeMs;

			return {
				maker,
				totalQuoteValueOnBook: isFresh
					? getQuoteValueOnBook(fillEvent, side, quoteBlob, spotPrecision)
					: 0,
				competitiveQuoteValueOnBook:
					isFresh && competitiveLiquidity ? competitiveLiquidity.quoteValue : 0,
				competitiveLiquidity: isFresh ? competitiveLiquidity : undefined,
			};
		});
	}

	const mmSetKey = `market_mms_${fillEvent.marketType}_${fillEvent.marketIndex}`;
	const makers = await indicativeQuotesRedisClient.smembers(mmSetKey);

	if (!makers.length) {
		marketQuoteCache.set(marketKey, {
			fetchedAtMs: Date.now(),
			quoteBlobs: [],
		});
		return [];
	}

	const quoteBlobs = (await Promise.all(
		makers.map((maker) =>
			indicativeQuotesRedisClient.get(
				`mm_quotes_v2_${fillEvent.marketType}_${fillEvent.marketIndex}_${maker}`
			)
		)
	)) as (IndicativeQuoteBlob | null)[];

	const cachedQuoteBlobs = makers.map((maker, idx) => ({
		maker,
		quoteBlob: quoteBlobs[idx],
	}));

	marketQuoteCache.set(marketKey, {
		fetchedAtMs: Date.now(),
		quoteBlobs: cachedQuoteBlobs,
	});

	return cachedQuoteBlobs.map(({ maker, quoteBlob }) => {
		const spotPrecision =
			fillEvent.marketType === 'spot'
				? sdkConfig.SPOT_MARKETS[fillEvent.marketIndex].precision
				: undefined;
		const competitiveLiquidity = getCompetitiveLiquidity(
			maker,
			fillEvent,
			side,
			fillPrice,
			quoteBlob,
			spotPrecision
		);
		const quoteTsMs = getQuoteTimestampMs(quoteBlob);
		const quoteAgeMs =
			quoteTsMs !== undefined ? fillTsMs - quoteTsMs : Infinity;
		const isFresh = quoteAgeMs >= 0 && quoteAgeMs <= indicativeQuoteMaxAgeMs;

		return {
			maker,
			totalQuoteValueOnBook: isFresh
				? getQuoteValueOnBook(fillEvent, side, quoteBlob, spotPrecision)
				: 0,
			competitiveQuoteValueOnBook:
				isFresh && competitiveLiquidity ? competitiveLiquidity.quoteValue : 0,
			competitiveLiquidity: isFresh ? competitiveLiquidity : undefined,
		};
	});
};

const processFillEvent = async (
	fillEvent: FillEvent,
	redisClient: RedisClient,
	indicativeQuotesRedisClient: RedisClient
) => {
	redisClient.publish(
		`${redisClientPrefix}trades_${fillEvent.marketType}_${fillEvent.marketIndex}`,
		fillEvent
	);

	const fillSide = getFillSide(fillEvent);
	const fillPrice = getFillPrice(fillEvent);
	if (
		!fillSide ||
		!fillPrice ||
		!Number.isFinite(fillPrice) ||
		fillPrice <= 0
	) {
		return;
	}

	const fillTsMs = getFillTimestampMs(fillEvent.ts);
	const marketMetricAttrs = {
		market_index: fillEvent.marketIndex,
		market_type: fillEvent.marketType,
		side: fillSide,
	};
	marketFillCount.add(1, marketMetricAttrs);

	try {
		const marketQuoteEvaluations = await getMarketQuotes(
			indicativeQuotesRedisClient,
			fillEvent,
			fillSide,
			fillPrice,
			fillTsMs
		);
		const marketQuotes = marketQuoteEvaluations
			.map((evaluation) => evaluation.competitiveLiquidity)
			.filter((quote): quote is CompetitiveLiquidity => !!quote);

		for (const evaluation of marketQuoteEvaluations) {
			const attrs = getMakerMetricAttrs(fillEvent, evaluation.maker, fillSide);
			const hasPresence = evaluation.totalQuoteValueOnBook > 0;

			if (hasPresence) {
				indicativePresenceCount.add(1, attrs);
			}

			indicativeTotalSizeOnBookGauge.setLatestValue(
				evaluation.totalQuoteValueOnBook,
				attrs
			);
			indicativeCompetitiveSizeOnBookGauge.setLatestValue(
				evaluation.competitiveQuoteValueOnBook,
				attrs
			);
		}

		for (const quote of marketQuotes) {
			const attrs = getMakerMetricAttrs(fillEvent, quote.maker, fillSide);
			const opportunitySize = Math.min(
				fillEvent.baseAssetAmountFilled,
				quote.size
			);
			const opportunityNotional = opportunitySize * fillPrice;

			indicativeCompetitiveOpportunityCount.add(1, attrs);
			indicativeCompetitiveOpportunityNotional.add(opportunityNotional, attrs);
			indicativeQuoteEvaluationCount.add(1, {
				...attrs,
				result: 'competitive',
			});

			if (fillEvent.maker === quote.maker) {
				indicativeCompetitiveFillCount.add(1, attrs);
				indicativeCompetitiveCapturedNotional.add(
					Math.min(fillEvent.baseAssetAmountFilled, opportunitySize) *
						fillPrice,
					attrs
				);
				indicativeFillVsQuoteBucketCount.add(1, {
					...attrs,
					bucket: getIndicativeBpsBucket(
						getAbsoluteBpsDiff(fillPrice, quote.bestPrice)
					),
				});
			}
		}

		if (!marketQuotes.length) {
			indicativeQuoteEvaluationCount.add(1, {
				...marketMetricAttrs,
				maker: 'all',
				result: 'no_competitive_quotes',
			});
		}

		if (
			fillEvent.maker &&
			!marketQuotes.find((q) => q.maker === fillEvent.maker)
		) {
			indicativeQuoteEvaluationCount.add(1, {
				...getMakerMetricAttrs(fillEvent, fillEvent.maker, fillSide),
				result: 'maker_not_competitive',
			});
		}
	} catch (error) {
		logger.error('Error evaluating competitive indicative quotes:', error);
		indicativeQuoteEvaluationCount.add(1, {
			...marketMetricAttrs,
			maker: 'all',
			result: 'error',
		});
	}
};

const startMockFillEndpoint = (
	redisClient: RedisClient,
	indicativeQuotesRedisClient: RedisClient
) => {
	if (!enableMockFillEndpoint) {
		return;
	}

	const app = express();
	app.use(express.json());
	app.post('/mockFill', async (req, res) => {
		try {
			await processFillEvent(
				req.body as FillEvent,
				redisClient,
				indicativeQuotesRedisClient
			);
			res.status(200).json({ ok: true });
		} catch (error) {
			logger.error('Failed to process mock fill:', error);
			res.status(500).json({ ok: false });
		}
	});
	app.listen(mockFillPort, () => {
		logger.info(
			`Mock fill endpoint listening on http://localhost:${mockFillPort}`
		);
	});
};

const main = async () => {
	const wallet = new Wallet(new Keypair());
	const clearingHousePublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);

	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription: {
			type: 'websocket',
			commitment: stateCommitment,
			resubTimeoutMs: 30_000,
		},
		env: driftEnv,
	});

	const redisClient = new RedisClient({ prefix: redisClientPrefix });
	await redisClient.connect();
	const indicativeQuotesRedisClient = new RedisClient({});
	await indicativeQuotesRedisClient.connect();

	startMockFillEndpoint(redisClient, indicativeQuotesRedisClient);

	if (mockOnlyMode) {
		logger.info('Running in MOCK_ONLY_MODE; skipping chain subscriptions');
		return;
	}

	const slotSubscriber = new SlotSubscriber(connection, {
		resubTimeoutMs: 10_000,
	});

	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`DriftClient ProgramId: ${driftClient.program.programId.toBase58()}`
	);
	logger.info(`Wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(` . SOL balance: ${lamportsBalance / 10 ** 9}`);

	await driftClient.subscribe();
	driftClient.eventEmitter.on('error', (e) => {
		logger.error(e);
	});

	await slotSubscriber.subscribe();

	const eventSubscriber = new EventSubscriber(connection, driftClient.program, {
		maxTx: 8192,
		maxEventsPerType: 4096,
		orderBy: 'client',
		commitment: 'confirmed',
		logProviderConfig: {
			type: 'polling',
			frequency: 1000,
		},
	});

	await eventSubscriber.subscribe();

	const eventObservable = fromEvent(eventSubscriber.eventEmitter, 'newEvent');
	eventObservable
		.pipe(
			filter(
				(event) =>
					event.eventType === 'OrderActionRecord' &&
					JSON.stringify(event.action) === JSON.stringify(OrderAction.FILL)
			),
			map((fill: Event<OrderActionRecord>) => {
				const basePrecision =
					getVariant(fill.marketType) === 'spot'
						? sdkConfig.SPOT_MARKETS[fill.marketIndex].precision
						: BASE_PRECISION;
				return {
					ts: fill.ts.toNumber(),
					marketIndex: fill.marketIndex,
					marketType: getVariant(fill.marketType),
					filler: fill.filler?.toBase58(),
					takerFee: convertToNumber(fill.takerFee, QUOTE_PRECISION),
					makerFee: convertToNumber(fill.makerFee, QUOTE_PRECISION),
					quoteAssetAmountSurplus: convertToNumber(
						fill.quoteAssetAmountSurplus,
						QUOTE_PRECISION
					),
					baseAssetAmountFilled: convertToNumber(
						fill.baseAssetAmountFilled,
						basePrecision
					),
					quoteAssetAmountFilled: convertToNumber(
						fill.quoteAssetAmountFilled,
						QUOTE_PRECISION
					),
					taker: fill.taker?.toBase58(),
					takerOrderId: fill.takerOrderId,
					takerOrderDirection: fill.takerOrderDirection
						? getVariant(fill.takerOrderDirection)
						: undefined,
					takerOrderBaseAssetAmount: convertToNumber(
						fill.takerOrderBaseAssetAmount,
						basePrecision
					),
					takerOrderCumulativeBaseAssetAmountFilled: convertToNumber(
						fill.takerOrderCumulativeBaseAssetAmountFilled,
						basePrecision
					),
					takerOrderCumulativeQuoteAssetAmountFilled: convertToNumber(
						fill.takerOrderCumulativeQuoteAssetAmountFilled,
						QUOTE_PRECISION
					),
					maker: fill.maker?.toBase58(),
					makerOrderId: fill.makerOrderId,
					makerOrderDirection: fill.makerOrderDirection
						? getVariant(fill.makerOrderDirection)
						: undefined,
					makerOrderBaseAssetAmount: convertToNumber(
						fill.makerOrderBaseAssetAmount,
						basePrecision
					),
					makerOrderCumulativeBaseAssetAmountFilled: convertToNumber(
						fill.makerOrderCumulativeBaseAssetAmountFilled,
						basePrecision
					),
					makerOrderCumulativeQuoteAssetAmountFilled: convertToNumber(
						fill.makerOrderCumulativeQuoteAssetAmountFilled,
						QUOTE_PRECISION
					),
					oraclePrice: convertToNumber(fill.oraclePrice, PRICE_PRECISION),
					txSig: fill.txSig,
					slot: fill.slot,
					fillRecordId: fill.fillRecordId?.toNumber(),
					action: 'fill',
					actionExplanation: getVariant(fill.actionExplanation),
					referrerReward: convertToNumber(
						new BN(fill.referrerReward ?? ZERO),
						QUOTE_PRECISION
					),
					bitFlags: fill.bitFlags,
				};
			})
		)
		.subscribe(async (fillEvent: FillEvent) => {
			await processFillEvent(
				fillEvent,
				redisClient,
				indicativeQuotesRedisClient
			);
		});

	console.log('Publishing trades');
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
