import { logger } from '@drift/common';
import { RedisClient } from '@drift/common/clients';
import {
	BN,
	DLOBSubscriber,
	MarketType,
	Order,
	OrderStatus,
	OrderTriggerCondition,
	OrderType,
	PositionDirection,
	ZERO,
} from '@drift-labs/sdk';

interface OrderbookLevel {
	price: string;
	size: string;
	sources: Record<string, string>;
}

interface Orderbook {
	marketIndex: number;
	bids: OrderbookLevel[];
	asks: OrderbookLevel[];
	slot: number;
	oracleData?: {
		price: string;
		slot: string;
		confidence: string;
		hasSufficientNumberOfDataPoints: boolean;
		twap: string;
		twapConfidence: string;
		maxPrice?: string;
	};
}

interface OrderbookDelta {
	m: number;
	slot: number;
	startSlot: number;
	t: number;
	b: [string, string, Record<string, string>][];
	a: [string, string, Record<string, string>][];
	f?: boolean;
	oracleData?: {
		price: string;
		slot: string;
		confidence: string;
		hasSufficientNumberOfDataPoints: boolean;
		twap: string;
		twapConfidence: string;
		maxPrice?: string;
	};
}

const INDICATIVE_QUOTES_PUBKEY = 'inDNdu3ML4vG5LNExqcwuCQtLcCU8KfK5YM2qYV3JJz';

export const OrderbookDeltaTracker = ({
	redisClient,
	indicativeRedisClient,
	marketType = MarketType.PERP,
	publishDiffs = false,
}: {
	redisClient: RedisClient;
	indicativeRedisClient: RedisClient;
	marketType: MarketType;
	publishDiffs: boolean;
}) => {
	const currentOrderbooks: Map<number, Orderbook> = new Map();
	const redisClientPrefix = redisClient.getPrefix();
	const serialisedMarketType = marketType === MarketType.PERP ? 'perp' : 'spot';
	const redisChannelPrefix = `orderbook_${serialisedMarketType}_`;
	const snapshotSent = new Set<number>();

	const lastPublishedSlots: Map<number, number> = new Map();

	const processOrderbook = async (newOrderbook: Orderbook): Promise<void> => {
		const { marketIndex } = newOrderbook;
		const currentOrderbook = currentOrderbooks.get(marketIndex);
		
		if (!lastPublishedSlots.has(marketIndex)) {
			lastPublishedSlots.set(marketIndex, 0);
		}

		if (!currentOrderbook) {
			currentOrderbooks.set(marketIndex, deepCloneOrderbook(newOrderbook));
			await storeSnapshot(newOrderbook);

			if (!snapshotSent.has(marketIndex)) {
				if (!publishDiffs) {
					await publishBook(newOrderbook);
				} else {
					await publishInitialSnapshot(newOrderbook);
				}

				snapshotSent.add(marketIndex);
				lastPublishedSlots.set(marketIndex, newOrderbook.slot);
			}
			return;
		}

		if (newOrderbook.slot <= currentOrderbook.slot) {
			logger.info(
				`Skipping outdated orderbook for market ${marketIndex}, slot ${newOrderbook.slot} <= ${currentOrderbook.slot}`
			);
			return;
		}

		const delta = computeDelta(currentOrderbook, newOrderbook);

		if (hasDeltaChanges(delta)) {
			await storeSnapshot(newOrderbook);

			const lastPublishedSlot = lastPublishedSlots.get(marketIndex) || 0;
			delta.startSlot = lastPublishedSlot;
			
			if (publishDiffs) {
				await publishDelta(delta);
			} else {
				await publishBook(newOrderbook);
			}
			
			lastPublishedSlots.set(marketIndex, newOrderbook.slot);
		}

		currentOrderbooks.set(marketIndex, deepCloneOrderbook(newOrderbook));
	};

	const computeDelta = (prev: Orderbook, next: Orderbook): OrderbookDelta => {
		const delta: OrderbookDelta = {
			m: next.marketIndex,
			slot: next.slot,
			startSlot: prev.slot,
			t: Date.now(),
			b: [] as [string, string, Record<string, string>][],
			a: [] as [string, string, Record<string, string>][],
			oracleData: next.oracleData,
		};

		const prevBidMap = new Map<
			string,
			{ size: string; sources: Record<string, string> }
		>();
		const prevAskMap = new Map<
			string,
			{ size: string; sources: Record<string, string> }
		>();
		const nextBidMap = new Map<
			string,
			{ size: string; sources: Record<string, string> }
		>();
		const nextAskMap = new Map<
			string,
			{ size: string; sources: Record<string, string> }
		>();

		prev.bids.forEach((level) =>
			prevBidMap.set(level.price, { size: level.size, sources: level.sources })
		);
		prev.asks.forEach((level) =>
			prevAskMap.set(level.price, { size: level.size, sources: level.sources })
		);
		next.bids.forEach((level) =>
			nextBidMap.set(level.price, { size: level.size, sources: level.sources })
		);
		next.asks.forEach((level) =>
			nextAskMap.set(level.price, { size: level.size, sources: level.sources })
		);

		prevBidMap.forEach((data, price) => {
			if (!nextBidMap.has(price)) {
				delta.b.push([price, '0', {}]);
			} else {
				const nextData = nextBidMap.get(price)!;
				const sizeChanged = nextData.size !== data.size;
				const sourcesChanged = !areSourcesEqual(data.sources, nextData.sources);

				if (sizeChanged || sourcesChanged) {
					delta.b.push([price, nextData.size, nextData.sources]);
				}
			}
		});

		nextBidMap.forEach((data, price) => {
			if (!prevBidMap.has(price)) {
				delta.b.push([price, data.size, data.sources]);
			}
		});

		prevAskMap.forEach((data, price) => {
			if (!nextAskMap.has(price)) {
				delta.a.push([price, '0', {}]);
			} else {
				const nextData = nextAskMap.get(price)!;
				const sizeChanged = nextData.size !== data.size;
				const sourcesChanged = !areSourcesEqual(data.sources, nextData.sources);

				if (sizeChanged || sourcesChanged) {
					delta.a.push([price, nextData.size, nextData.sources]);
				}
			}
		});

		nextAskMap.forEach((data, price) => {
			if (!prevAskMap.has(price)) {
				delta.a.push([price, data.size, data.sources]);
			}
		});

		return delta;
	};

	const areSourcesEqual = (
		sources1: Record<string, string>,
		sources2: Record<string, string>
	): boolean => {
		const keys1 = Object.keys(sources1);
		const keys2 = Object.keys(sources2);

		if (keys1.length !== keys2.length) {
			return false;
		}

		return keys1.every(
			(key) => sources2.hasOwnProperty(key) && sources1[key] === sources2[key]
		);
	};

	const isOracleEqual = (
		oracle1?: {
			price: string;
			slot: string;
			confidence: string;
			twap: string;
			twapConfidence: string;
			hasSufficientNumberOfDataPoints: boolean;
			maxPrice?: string;
		},
		oracle2?: {
			price: string;
			slot: string;
			confidence: string;
			twap: string;
			twapConfidence: string;
			hasSufficientNumberOfDataPoints: boolean;
			maxPrice?: string;
		}
	): boolean => {
		if (!oracle1 && !oracle2) return true;
		if (!oracle1 || !oracle2) return false;

		return (
			oracle1.price === oracle2.price &&
			oracle1.slot === oracle2.slot &&
			oracle1.confidence === oracle2.confidence &&
			oracle1.twap === oracle2.twap &&
			oracle1.twapConfidence === oracle2.twapConfidence &&
			oracle1.hasSufficientNumberOfDataPoints ===
				oracle2.hasSufficientNumberOfDataPoints &&
			((!oracle1.maxPrice && !oracle2.maxPrice) ||
				oracle1.maxPrice === oracle2.maxPrice)
		);
	};

	const hasDeltaChanges = (delta: OrderbookDelta): boolean => {
		const hasOrderbookChanges = delta.b.length > 0 || delta.a.length > 0;

		const hasOracleChanges =
			delta.oracleData &&
			(!currentOrderbooks.get(delta.m)?.oracleData ||
				!isOracleEqual(
					currentOrderbooks.get(delta.m)?.oracleData,
					delta.oracleData
				));

		return hasOrderbookChanges || hasOracleChanges;
	};

	const storeSnapshot = async (orderbook: Orderbook): Promise<void> => {
		try {
			const lastPublishedSlot = lastPublishedSlots.get(orderbook.marketIndex) || 0;

			const message: OrderbookDelta = {
				m: orderbook.marketIndex,
				slot: orderbook.slot,
				startSlot: lastPublishedSlot,
				t: Date.now(),
				b: orderbook.bids.map((bid) => [bid.price, bid.size, bid.sources]),
				a: orderbook.asks.map((ask) => [ask.price, ask.size, ask.sources]),
				f: true,
				oracleData: orderbook.oracleData,
			};

			const snapshotKey = `${redisChannelPrefix}${orderbook.marketIndex}_snapshot`;
			await redisClient
				.forceGetClient()
				.setex(snapshotKey, 3600, JSON.stringify(message));

			logger.info(
				`Stored orderbook snapshot in Redis for market ${orderbook.marketIndex}, slot ${orderbook.slot}, startSlot ${lastPublishedSlot}`
			);
		} catch (error) {
			logger.error(`Failed to store orderbook snapshot: ${error.message}`);
		}
	};

	const publishInitialSnapshot = async (
		orderbook: Orderbook
	): Promise<void> => {
		try {
			const lastPublishedSlot = lastPublishedSlots.get(orderbook.marketIndex) || 0;

			const channel = `${redisClientPrefix}${redisChannelPrefix}${orderbook.marketIndex}_delta`;

			const message: OrderbookDelta = {
				m: orderbook.marketIndex,
				slot: orderbook.slot,
				startSlot: lastPublishedSlot,
				t: Date.now(),
				b: orderbook.bids.map((bid) => [bid.price, bid.size, bid.sources]),
				a: orderbook.asks.map((ask) => [ask.price, ask.size, ask.sources]),
				f: true,
				oracleData: orderbook.oracleData,
			};

			await redisClient.publish(channel, message);

			logger.info(
				`Published INITIAL orderbook snapshot for market ${orderbook.marketIndex}, slot ${orderbook.slot}, startSlot ${lastPublishedSlot}`
			);
		} catch (error) {
			logger.error(
				`Failed to publish initial orderbook snapshot: ${error.message}`
			);
		}
	};

	const publishDelta = async (delta: OrderbookDelta): Promise<void> => {
		try {
			const channel = `${redisClientPrefix}${redisChannelPrefix}${delta.m}_delta`;

			delta.f = false;

			await redisClient.publish(channel, delta);

			const messageSize = JSON.stringify(delta).length;
			const bidChanges = delta.b.length;
			const askChanges = delta.a.length;

			logger.info(
				`Published orderbook delta (${messageSize} bytes) for market ${delta.m} with ${bidChanges} bid changes and ${askChanges} ask changes, slot range: ${delta.startSlot} -> ${delta.slot}`
			);
		} catch (error) {
			logger.error(`Failed to publish orderbook delta: ${error.message}`);
		}
	};

	const publishBook = async (orderbook: Orderbook): Promise<void> => {
		try {
			const channel = `${redisClientPrefix}${redisChannelPrefix}${orderbook.marketIndex}`;
			await redisClient.publish(channel, orderbook);
		} catch (error) {
			logger.error(`Failed to publish orderbook delta: ${error.message}`);
		}
	};

	const deepCloneOrderbook = (orderbook: Orderbook): Orderbook => {
		return {
			marketIndex: orderbook.marketIndex,
			bids: orderbook.bids.map((level) => ({
				price: level.price,
				size: level.size,
				sources: { ...level.sources },
			})),
			asks: orderbook.asks.map((level) => ({
				price: level.price,
				size: level.size,
				sources: { ...level.sources },
			})),
			slot: orderbook.slot,
			oracleData: orderbook.oracleData
				? { ...orderbook.oracleData }
				: undefined,
		};
	};

	const addIndicativeLiquidity = async (
		dlobSubscriber: DLOBSubscriber,
		marketIndex: number
	) => {
		const mms = await indicativeRedisClient.smembers(
			`market_mms_${serialisedMarketType}_${marketIndex}`
		);
		const mmQuotes = await Promise.all(
			mms.map((mm) => {
				return indicativeRedisClient.get(
					`mm_quotes_${serialisedMarketType}_${marketIndex}_${mm}`
				);
			})
		);

		const nowMinus1000Ms = Date.now() - 1000;
		mmQuotes.forEach((quote) => {
			if (Number(quote['ts']) > nowMinus1000Ms) {
				const indicativeBaseOrder: Order = {
					status: OrderStatus.OPEN,
					orderType: OrderType.LIMIT,
					orderId: 0,
					slot: new BN(dlobSubscriber.slotSource.getSlot()),
					marketIndex: marketIndex,
					marketType,
					baseAssetAmount: ZERO,
					immediateOrCancel: false,
					direction: PositionDirection.LONG,
					oraclePriceOffset: 0,
					maxTs: new BN(quote['ts'] + 1000),
					reduceOnly: false,
					triggerCondition: OrderTriggerCondition.ABOVE,
					price: ZERO,
					userOrderId: 0,
					postOnly: true,
					auctionDuration: 0,
					auctionStartPrice: ZERO,
					auctionEndPrice: ZERO,

					existingPositionDirection: PositionDirection.LONG,
					triggerPrice: ZERO,
					baseAssetAmountFilled: ZERO,
					quoteAssetAmountFilled: ZERO,
					quoteAssetAmount: ZERO,
					bitFlags: 0,
					postedSlotTail: 0,
				};

				if (quote['bid_size'] && quote['bid_price']) {
					const indicativeBid: Order = Object.assign({}, indicativeBaseOrder, {
						oraclePriceOffset: quote['is_oracle_offset']
							? quote['bid_price']
							: 0,
						price: quote['is_oracle_offset'] ? 0 : new BN(quote['bid_price']),
						baseAssetAmount: new BN(quote['bid_size']),
						direction: PositionDirection.LONG,
					});

					dlobSubscriber.dlob.insertOrder(
						indicativeBid,
						INDICATIVE_QUOTES_PUBKEY,
						dlobSubscriber.slotSource.getSlot(),
						false
					);
				}

				if (quote['ask_size'] && quote['ask_price']) {
					const indicativeAsk: Order = Object.assign({}, indicativeBaseOrder, {
						oraclePriceOffset: quote['is_oracle_offset']
							? quote['ask_price']
							: 0,
						price: quote['is_oracle_offset'] ? 0 : new BN(quote['ask_price']),
						baseAssetAmount: new BN(quote['ask_size']),
						direction: PositionDirection.SHORT,
					});
					dlobSubscriber.dlob.insertOrder(
						indicativeAsk,
						INDICATIVE_QUOTES_PUBKEY,
						dlobSubscriber.slotSource.getSlot(),
						false
					);
				}
			}
		});
	};

	return {
		processOrderbook,
		computeDelta,
		hasDeltaChanges,
		storeSnapshot,
		publishInitialSnapshot,
		publishDelta,
		deepCloneOrderbook,
		addIndicativeLiquidity,
	};
};