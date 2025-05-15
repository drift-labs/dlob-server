import { logger } from '@drift/common';
import { RedisClient } from '@drift/common/clients';

export interface OrderbookLevel {
	price: string;
	size: string;
	sources: Record<string, string>;
}

export interface Orderbook {
	marketIndex: number;
	bids: OrderbookLevel[];
	asks: OrderbookLevel[];
	slot: number;
}

export interface OrderbookDelta {
	m: number;
	slot: number;
	t: number;
	b: [string, string, Record<string, string>][];
	a: [string, string, Record<string, string>][];
	f?: boolean;
}

export const OrderbookDeltaTracker = (redisClient: RedisClient) => {
	const currentOrderbooks: Map<number, Orderbook> = new Map();
    const redisClientPrefix = redisClient.getPrefix()
	const redisChannelPrefix = `orderbook_perp_`;
	const snapshotSent = new Set<number>();

	const processOrderbook = async (newOrderbook: Orderbook): Promise<void> => {
		const { marketIndex } = newOrderbook;
		const currentOrderbook = currentOrderbooks.get(marketIndex);

		if (!currentOrderbook) {
			currentOrderbooks.set(marketIndex, deepCloneOrderbook(newOrderbook));

			await storeSnapshot(newOrderbook);

			if (!snapshotSent.has(marketIndex)) {
				await publishInitialSnapshot(newOrderbook);
				snapshotSent.add(marketIndex);
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
			await publishDelta(delta, marketIndex);
		} else {
			logger.info(`No changes detected for market ${marketIndex}`);
		}

		currentOrderbooks.set(marketIndex, deepCloneOrderbook(newOrderbook));
	};

	const computeDelta = (prev: Orderbook, next: Orderbook): OrderbookDelta => {
		const delta: OrderbookDelta = {
			m: next.marketIndex,
			slot: next.slot,
			t: Date.now(),
			b: [] as [string, string, Record<string, string>][],
			a: [] as [string, string, Record<string, string>][],
		};

		const prevBidMap = new Map<string, { size: string; sources: Record<string, string> }>();
		const prevAskMap = new Map<string, { size: string; sources: Record<string, string> }>();
		const nextBidMap = new Map<string, { size: string; sources: Record<string, string> }>();
		const nextAskMap = new Map<string, { size: string; sources: Record<string, string> }>();

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

	const hasDeltaChanges = (delta: OrderbookDelta): boolean => {
		return delta.b.length > 0 || delta.a.length > 0;
	};

	const storeSnapshot = async (orderbook: Orderbook): Promise<void> => {
		try {
			const message: OrderbookDelta = {
				m: orderbook.marketIndex,
				slot: orderbook.slot,
				t: Date.now(),
				b: orderbook.bids.map((bid) => [bid.price, bid.size, bid.sources]),
				a: orderbook.asks.map((ask) => [ask.price, ask.size, ask.sources]),
				f: true,
			};

			const snapshotKey = `${redisChannelPrefix}${orderbook.marketIndex}_snapshot`;
			await redisClient.forceGetClient().setex(snapshotKey, 3600, JSON.stringify(message));

			logger.info(
				`Stored orderbook snapshot in Redis for market ${orderbook.marketIndex}, slot ${orderbook.slot}`
			);
		} catch (error) {
			logger.error(`Failed to store orderbook snapshot: ${error.message}`);
		}
	};

	const publishInitialSnapshot = async (orderbook: Orderbook): Promise<void> => {
		try {
			const channel = `${redisClientPrefix}${redisChannelPrefix}${orderbook.marketIndex}_snapshot`;

			const message = {
				m: orderbook.marketIndex,
				s: orderbook.slot,
				t: Date.now(),
				b: orderbook.bids.map((bid) => [bid.price, bid.size, bid.sources]),
				a: orderbook.asks.map((ask) => [ask.price, ask.size, ask.sources]),
				f: true,
			};

			await redisClient.publish(channel, message);

			logger.info(
				`Published INITIAL orderbook snapshot for market ${orderbook.marketIndex}, slot ${orderbook.slot}`
			);
		} catch (error) {
			logger.error(`Failed to publish initial orderbook snapshot: ${error.message}`);
		}
	};

	const publishDelta = async (delta: OrderbookDelta, marketIndex: number): Promise<void> => {
		try {
			const channel = `${redisClientPrefix}${redisChannelPrefix}${marketIndex}_delta`;

			delta.f = false;

			await redisClient.publish(channel, delta);

			const messageSize = JSON.stringify(delta).length;
			const bidChanges = delta.b.length;
			const askChanges = delta.a.length;

			logger.info(
				`Published orderbook delta (${messageSize} bytes) for market ${marketIndex} with ${bidChanges} bid changes and ${askChanges} ask changes`
			);
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
		};
	};

	return {
		processOrderbook,
		computeDelta,
		hasDeltaChanges,
		storeSnapshot,
		publishInitialSnapshot,
		publishDelta,
		deepCloneOrderbook,
	};
};
