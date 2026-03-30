import {
	CompetitiveLiquidity,
	getAbsoluteBpsDiff,
	getCompetitiveLiquidity,
	getIndicativeBpsBucket,
	getIndicativeDirectionBucket,
	getFillPrice,
	getFillSide,
	getMakerMetricAttrs,
	getQuoteTimestampMs,
	getQuoteValueOnBook,
	getSignedBpsDiff,
	IndicativeQuoteBlob,
} from './tradeMetrics';

export type FillEvent = {
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

type CounterSink = {
	add(value: number, attributes: Record<string, string | number>): void;
};

type GaugeSink = {
	setLatestValue(
		value: number,
		attributes: Record<string, string | number>
	): void;
};

export type TradeMetricsSinks = {
	marketFillCount: CounterSink;
	indicativePresenceCount: CounterSink;
	indicativeCompetitiveOpportunityCount: CounterSink;
	indicativeCompetitiveFillCount: CounterSink;
	indicativeCompetitiveOpportunityNotional: CounterSink;
	indicativeCompetitiveCapturedNotional: CounterSink;
	indicativeFillVsQuoteBucketCount: CounterSink;
	indicativeFillVsQuoteDirectionBucketCount: CounterSink;
	indicativeQuoteEvaluationCount: CounterSink;
	indicativeTotalSizeOnBookGauge: GaugeSink;
	indicativeCompetitiveSizeOnBookGauge: GaugeSink;
};

export type PublisherRedisClient = {
	publish(key: string, value: any): Promise<number> | number;
};

export type IndicativeQuotesRedisClient = {
	smembers(key: string): Promise<string[]>;
	get<T>(key: string): Promise<T>;
};

export const createTradeMetricsProcessor = ({
	redisClientPrefix,
	indicativeQuoteMaxAgeMs,
	indicativeQuotesCacheTtlMs,
	spotMarketPrecisionResolver,
	publisherRedisClient,
	indicativeQuotesRedisClient,
	metrics,
	nowMsProvider = Date.now,
	onError,
}: {
	redisClientPrefix: string;
	indicativeQuoteMaxAgeMs: number;
	indicativeQuotesCacheTtlMs: number;
	spotMarketPrecisionResolver: (marketIndex: number) => number | undefined;
	publisherRedisClient: PublisherRedisClient;
	indicativeQuotesRedisClient: IndicativeQuotesRedisClient;
	metrics: TradeMetricsSinks;
	nowMsProvider?: () => number;
	onError?: (error: unknown) => void;
}) => {
	const marketQuoteCache = new Map<string, MarketQuoteCacheEntry>();

	const getMarketQuotes = async (
		fillEvent: FillEvent,
		side: 'long' | 'short',
		fillPrice: number,
		evaluationTsMs: number
	): Promise<MarketQuoteEvaluation[]> => {
		const marketKey = `${fillEvent.marketType}_${fillEvent.marketIndex}_${side}`;
		const mapQuoteBlob = (
			maker: string,
			quoteBlob: IndicativeQuoteBlob | null
		): MarketQuoteEvaluation => {
			const spotPrecision =
				fillEvent.marketType === 'spot'
					? spotMarketPrecisionResolver(fillEvent.marketIndex)
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
				quoteTsMs !== undefined ? evaluationTsMs - quoteTsMs : Infinity;
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
		};

		const cached = marketQuoteCache.get(marketKey);
		if (
			cached &&
			nowMsProvider() - cached.fetchedAtMs <= indicativeQuotesCacheTtlMs
		) {
			return cached.quoteBlobs.map(({ maker, quoteBlob }) =>
				mapQuoteBlob(maker, quoteBlob)
			);
		}

		const mmSetKey = `market_mms_${fillEvent.marketType}_${fillEvent.marketIndex}`;
		const makers = await indicativeQuotesRedisClient.smembers(mmSetKey);
		if (!makers.length) {
			marketQuoteCache.set(marketKey, {
				fetchedAtMs: nowMsProvider(),
				quoteBlobs: [],
			});
			return [];
		}

		const quoteBlobs = (await Promise.all(
			makers.map((maker) =>
				indicativeQuotesRedisClient.get<IndicativeQuoteBlob | null>(
					`mm_quotes_v2_${fillEvent.marketType}_${fillEvent.marketIndex}_${maker}`
				)
			)
		)) as (IndicativeQuoteBlob | null)[];

		const cachedQuoteBlobs = makers.map((maker, idx) => ({
			maker,
			quoteBlob: quoteBlobs[idx],
		}));
		marketQuoteCache.set(marketKey, {
			fetchedAtMs: nowMsProvider(),
			quoteBlobs: cachedQuoteBlobs,
		});

		return cachedQuoteBlobs.map(({ maker, quoteBlob }) =>
			mapQuoteBlob(maker, quoteBlob)
		);
	};

	const processFillEvent = async (fillEvent: FillEvent) => {
		await publisherRedisClient.publish(
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

		const marketMetricAttrs = {
			market_index: fillEvent.marketIndex,
			market_type: fillEvent.marketType,
			side: fillSide,
		};
		metrics.marketFillCount.add(1, marketMetricAttrs);

		try {
			const evaluationTsMs = nowMsProvider();
			const marketQuoteEvaluations = await getMarketQuotes(
				fillEvent,
				fillSide,
				fillPrice,
				evaluationTsMs
			);
			const marketQuotes = marketQuoteEvaluations
				.map((evaluation) => evaluation.competitiveLiquidity)
				.filter((quote): quote is CompetitiveLiquidity => !!quote);

			for (const evaluation of marketQuoteEvaluations) {
				const attrs = getMakerMetricAttrs(
					fillEvent,
					evaluation.maker,
					fillSide
				);
				if (evaluation.totalQuoteValueOnBook > 0) {
					metrics.indicativePresenceCount.add(1, attrs);
				}
				metrics.indicativeTotalSizeOnBookGauge.setLatestValue(
					evaluation.totalQuoteValueOnBook,
					attrs
				);
				metrics.indicativeCompetitiveSizeOnBookGauge.setLatestValue(
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
				metrics.indicativeCompetitiveOpportunityCount.add(1, attrs);
				metrics.indicativeCompetitiveOpportunityNotional.add(
					opportunityNotional,
					attrs
				);
				metrics.indicativeQuoteEvaluationCount.add(1, {
					...attrs,
					result: 'competitive',
				});

				if (fillEvent.maker === quote.maker) {
					metrics.indicativeCompetitiveFillCount.add(1, attrs);
					metrics.indicativeCompetitiveCapturedNotional.add(
						Math.min(fillEvent.baseAssetAmountFilled, opportunitySize) *
							fillPrice,
						attrs
					);
					metrics.indicativeFillVsQuoteBucketCount.add(1, {
						...attrs,
						bucket: getIndicativeBpsBucket(
							getAbsoluteBpsDiff(fillPrice, quote.bestPrice)
						),
					});
					metrics.indicativeFillVsQuoteDirectionBucketCount.add(1, {
						...attrs,
						bucket: getIndicativeDirectionBucket(
							getSignedBpsDiff(fillPrice, quote.bestPrice)
						),
					});
				}
			}

			if (!marketQuotes.length) {
				metrics.indicativeQuoteEvaluationCount.add(1, {
					...marketMetricAttrs,
					maker: 'all',
					result: 'no_competitive_quotes',
				});
			}

			const fillMakerEvaluation = fillEvent.maker
				? marketQuoteEvaluations.find(
						(evaluation) => evaluation.maker === fillEvent.maker
				  )
				: undefined;
			if (
				fillEvent.maker &&
				fillMakerEvaluation &&
				fillMakerEvaluation.totalQuoteValueOnBook > 0 &&
				!marketQuotes.find((quote) => quote.maker === fillEvent.maker)
			) {
				metrics.indicativeQuoteEvaluationCount.add(1, {
					...getMakerMetricAttrs(fillEvent, fillEvent.maker, fillSide),
					result: 'maker_not_competitive',
				});
			}
		} catch (error) {
			onError?.(error);
			metrics.indicativeQuoteEvaluationCount.add(1, {
				...marketMetricAttrs,
				maker: 'all',
				result: 'error',
			});
		}
	};

	return {
		processFillEvent,
	};
};
