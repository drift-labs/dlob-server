import {
	BN,
	DLOBSubscriber,
	DLOBSubscriptionConfig,
	L2OrderBookGenerator,
	MarketType,
	groupL2,
	isVariant,
} from '@drift-labs/sdk';
import { RedisClient } from '../utils/redisClient';
import {
	SubscriberLookup,
	addMarketSlotToResponse,
	addOracletoResponse,
	l2WithBNToStrings,
} from '../utils/utils';

type wsMarketArgs = {
	marketIndex: number;
	marketType: MarketType;
	marketName: string;
	depth: number;
	includeVamm: boolean;
	numVammOrders?: number;
	grouping?: number;
	fallbackL2Generators?: L2OrderBookGenerator[];
	updateOnChange?: boolean;
};

const PERP_MAKRET_STALENESS_THRESHOLD = 10 * 60 * 1000;
const SPOT_MAKRET_STALENESS_THRESHOLD = 20 * 60 * 1000;

export type wsMarketInfo = {
	marketIndex: number;
	marketName: string;
};

export class DLOBSubscriberIO extends DLOBSubscriber {
	public marketArgs: wsMarketArgs[] = [];
	public lastSeenL2Formatted: Map<MarketType, Map<number, any>>;
	redisClient: RedisClient;
	public killSwitchSlotDiffThreshold: number;
	public lastMarketSlotMap: Map<
		MarketType,
		Map<number, { slot: number; ts: number }>
	>;

	constructor(
		config: DLOBSubscriptionConfig & {
			redisClient: RedisClient;
			perpMarketInfos: wsMarketInfo[];
			spotMarketInfos: wsMarketInfo[];
			spotMarketSubscribers: SubscriberLookup;
			killSwitchSlotDiffThreshold?: number;
		}
	) {
		super(config);
		this.redisClient = config.redisClient;
		this.killSwitchSlotDiffThreshold =
			config.killSwitchSlotDiffThreshold || 200;

		// Set up appropriate maps
		this.lastSeenL2Formatted = new Map();
		this.lastSeenL2Formatted.set(MarketType.SPOT, new Map());
		this.lastSeenL2Formatted.set(MarketType.PERP, new Map());
		this.lastMarketSlotMap = new Map();
		this.lastMarketSlotMap.set(MarketType.SPOT, new Map());
		this.lastMarketSlotMap.set(MarketType.PERP, new Map());

		for (const market of config.perpMarketInfos) {
			const perpMarket = this.driftClient.getPerpMarketAccount(
				market.marketIndex
			);
			const includeVamm = !isVariant(perpMarket.status, 'ammPaused');

			this.marketArgs.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.PERP,
				marketName: market.marketName,
				depth: -1,
				numVammOrders: 100,
				includeVamm,
				updateOnChange: false,
				fallbackL2Generators: [],
			});
		}
		for (const market of config.spotMarketInfos) {
			this.marketArgs.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.SPOT,
				marketName: market.marketName,
				depth: -1,
				includeVamm: false,
				updateOnChange: false,
				fallbackL2Generators: [
					config.spotMarketSubscribers[market.marketIndex].phoenix,
					config.spotMarketSubscribers[market.marketIndex].serum,
				].filter((a) => !!a),
			});
		}
	}

	override async updateDLOB(): Promise<void> {
		await super.updateDLOB();
		for (const marketArgs of this.marketArgs) {
			try {
				this.getL2AndSendMsg(marketArgs);
				this.getL3AndSendMsg(marketArgs);
			} catch (error) {
				console.error(error);
				console.log(`Error getting L2 ${marketArgs.marketName}`);
			}
		}
	}

	getL2AndSendMsg(marketArgs: wsMarketArgs): void {
		const grouping = marketArgs.grouping;
		const { marketName, ...l2FuncArgs } = marketArgs;
		const l2 = this.getL2(l2FuncArgs);
		const slot = l2.slot;
		const lastMarketSlotAndTime = this.lastMarketSlotMap
			.get(marketArgs.marketType)
			.get(marketArgs.marketIndex);
		if (!lastMarketSlotAndTime) {
			this.lastMarketSlotMap
				.get(marketArgs.marketType)
				.set(marketArgs.marketIndex, { slot, ts: Date.now() });
		}

		if (slot) {
			delete l2.slot;
		}
		const marketType = isVariant(marketArgs.marketType, 'perp')
			? 'perp'
			: 'spot';
		let l2Formatted: any;
		if (grouping) {
			const groupingBN = new BN(grouping);
			l2Formatted = l2WithBNToStrings(
				groupL2(l2, groupingBN, marketArgs.depth)
			);
		} else {
			l2Formatted = l2WithBNToStrings(l2);
		}

		if (marketArgs.updateOnChange) {
			if (
				this.lastSeenL2Formatted
					.get(marketArgs.marketType)
					?.get(marketArgs.marketIndex) === JSON.stringify(l2Formatted)
			)
				return;
		}

		this.lastSeenL2Formatted
			.get(marketArgs.marketType)
			?.set(marketArgs.marketIndex, JSON.stringify(l2Formatted));
		l2Formatted['marketName'] = marketName?.toUpperCase();
		l2Formatted['marketType'] = marketType?.toLowerCase();
		l2Formatted['marketIndex'] = marketArgs.marketIndex;
		l2Formatted['ts'] = Date.now();
		l2Formatted['slot'] = slot;
		addOracletoResponse(
			l2Formatted,
			this.driftClient,
			marketArgs.marketType,
			marketArgs.marketIndex
		);
		addMarketSlotToResponse(
			l2Formatted,
			this.driftClient,
			marketArgs.marketType,
			marketArgs.marketIndex
		);

		// Check if slot diffs are too large for oracle
		if (
			Math.abs(slot - parseInt(l2Formatted['oracleData']['slot'])) >
			this.killSwitchSlotDiffThreshold
		) {
			console.log(`Killing process due to slot diffs for market ${marketName}: 
				dlobProvider slot: ${slot}
				oracle slot: ${l2Formatted['oracleData']['slot']}
			`);
			process.exit(1);
		}

		// Check if times and slots are too different for market
		const MAKRET_STALENESS_THRESHOLD =
			marketType === 'perp'
				? PERP_MAKRET_STALENESS_THRESHOLD
				: SPOT_MAKRET_STALENESS_THRESHOLD;
		if (
			lastMarketSlotAndTime &&
			l2Formatted['marketSlot'] === lastMarketSlotAndTime.slot &&
			Date.now() - lastMarketSlotAndTime.ts > MAKRET_STALENESS_THRESHOLD
		) {
			console.log(`Killing process due to same slot for market ${marketName} after > ${MAKRET_STALENESS_THRESHOLD}ms: 
				dlobProvider slot: ${slot}
				market slot: ${l2Formatted['marketSlot']}
			`);
			process.exit(1);
		} else if (
			lastMarketSlotAndTime &&
			l2Formatted['marketSlot'] !== lastMarketSlotAndTime.slot
		) {
			console.log(
				`Updating market slot for ${marketArgs.marketName} with slot ${l2Formatted['marketSlot']}`
			);
			this.lastMarketSlotMap
				.get(marketArgs.marketType)
				.set(marketArgs.marketIndex, {
					slot: l2Formatted['marketSlot'],
					ts: Date.now(),
				});
		}

		const l2Formatted_depth100 = Object.assign({}, l2Formatted, {
			bids: l2Formatted.bids.slice(0, 100),
			asks: l2Formatted.asks.slice(0, 100),
		});
		const l2Formatted_depth20 = Object.assign({}, l2Formatted, {
			bids: l2Formatted.bids.slice(0, 20),
			asks: l2Formatted.asks.slice(0, 20),
		});
		const l2Formatted_depth5 = Object.assign({}, l2Formatted, {
			bids: l2Formatted.bids.slice(0, 5),
			asks: l2Formatted.asks.slice(0, 5),
		});

		this.redisClient.client.publish(
			`orderbook_${marketType}_${marketArgs.marketIndex}`,
			JSON.stringify(l2Formatted)
		);
		this.redisClient.client.set(
			`last_update_orderbook_${marketType}_${marketArgs.marketIndex}`,
			JSON.stringify(l2Formatted_depth100)
		);
		this.redisClient.client.set(
			`last_update_orderbook_${marketType}_${marketArgs.marketIndex}_depth_100`,
			JSON.stringify(l2Formatted_depth100)
		);
		this.redisClient.client.set(
			`last_update_orderbook_${marketType}_${marketArgs.marketIndex}_depth_20`,
			JSON.stringify(l2Formatted_depth20)
		);
		this.redisClient.client.set(
			`last_update_orderbook_${marketType}_${marketArgs.marketIndex}_depth_5`,
			JSON.stringify(l2Formatted_depth5)
		);
	}

	getL3AndSendMsg(marketArgs: wsMarketArgs): void {
		const { marketName, ...l2FuncArgs } = marketArgs;
		const l3 = this.getL3(l2FuncArgs);
		const slot = l3.slot;
		const lastMarketSlotAndTime = this.lastMarketSlotMap
			.get(marketArgs.marketType)
			.get(marketArgs.marketIndex);
		if (!lastMarketSlotAndTime) {
			this.lastMarketSlotMap
				.get(marketArgs.marketType)
				.set(marketArgs.marketIndex, { slot, ts: Date.now() });
		}

		if (slot) {
			delete l3.slot;
		}
		const marketType = isVariant(marketArgs.marketType, 'perp')
			? 'perp'
			: 'spot';

		for (const key of Object.keys(l3)) {
			for (const idx in l3[key]) {
				const level = l3[key][idx];
				l3[key][idx] = {
					...level,
					price: level.price.toString(),
					size: level.size.toString(),
				};
			}
		}

		l3['marketName'] = marketName?.toUpperCase();
		l3['marketType'] = marketType?.toLowerCase();
		l3['marketIndex'] = marketArgs.marketIndex;
		l3['ts'] = Date.now();
		l3['slot'] = slot;
		addOracletoResponse(
			l3,
			this.driftClient,
			marketArgs.marketType,
			marketArgs.marketIndex
		);
		addMarketSlotToResponse(
			l3,
			this.driftClient,
			marketArgs.marketType,
			marketArgs.marketIndex
		);

		// Check if slot diffs are too large for oracle
		if (
			Math.abs(slot - parseInt(l3['oracleData']['slot'])) >
			this.killSwitchSlotDiffThreshold
		) {
			console.log(`Killing process due to slot diffs for market ${marketName}: 
				dlobProvider slot: ${slot}
				oracle slot: ${l3['oracleData']['slot']}
			`);
			process.exit(1);
		}

		// Check if times and slots are too different for market
		const MAKRET_STALENESS_THRESHOLD =
			marketType === 'perp'
				? PERP_MAKRET_STALENESS_THRESHOLD
				: SPOT_MAKRET_STALENESS_THRESHOLD;
		if (
			lastMarketSlotAndTime &&
			l3['marketSlot'] === lastMarketSlotAndTime.slot &&
			Date.now() - lastMarketSlotAndTime.ts > MAKRET_STALENESS_THRESHOLD
		) {
			console.log(`Killing process due to same slot for market ${marketName} after > ${MAKRET_STALENESS_THRESHOLD}ms: 
				dlobProvider slot: ${slot}
				market slot: ${l3['marketSlot']}
			`);
			process.exit(1);
		} else if (
			lastMarketSlotAndTime &&
			l3['marketSlot'] !== lastMarketSlotAndTime.slot
		) {
			console.log(
				`Updating market slot for ${marketArgs.marketName} with slot ${l3['marketSlot']}`
			);
			this.lastMarketSlotMap
				.get(marketArgs.marketType)
				.set(marketArgs.marketIndex, {
					slot: l3['marketSlot'],
					ts: Date.now(),
				});
		}

		this.redisClient.client.set(
			`last_update_orderbook_l3_${marketType}_${marketArgs.marketIndex}`,
			JSON.stringify(l3)
		);
	}
}
