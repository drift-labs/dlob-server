import {
	BN,
	DLOBSubscriber,
	DLOBSubscriptionConfig,
	DevnetPerpMarkets,
	DevnetSpotMarkets,
	L2OrderBookGenerator,
	MainnetPerpMarkets,
	MainnetSpotMarkets,
	MarketType,
	groupL2,
	isVariant,
} from '@drift-labs/sdk';
import { driftEnv } from '../publishers/dlobPublisher';
import { RedisClient } from '../utils/redisClient';
import {
	SubscriberLookup,
	addOracletoResponse,
	l2WithBNToStrings,
} from '../utils/utils';

type wsMarketL2Args = {
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

export class DLOBSubscriberIO extends DLOBSubscriber {
	public marketL2Args: wsMarketL2Args[] = [];
	public lastSeenL2Formatted: Map<MarketType, Map<number, any>>;
	redisClient: RedisClient;

	constructor(
		config: DLOBSubscriptionConfig & {
			redisClient: RedisClient;
			spotMarketSubscribers: SubscriberLookup;
		}
	) {
		super(config);
		this.redisClient = config.redisClient;

		// Set up appropriate maps
		this.lastSeenL2Formatted = new Map();
		this.lastSeenL2Formatted.set(MarketType.SPOT, new Map());
		this.lastSeenL2Formatted.set(MarketType.PERP, new Map());

		// Add all active markets to the market L2Args
		const perpMarkets =
			driftEnv === 'devnet' ? DevnetPerpMarkets : MainnetPerpMarkets;
		const spotMarkets =
			driftEnv === 'devnet' ? DevnetSpotMarkets : MainnetSpotMarkets;

		for (const market of perpMarkets) {
			this.marketL2Args.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.PERP,
				marketName: market.symbol,
				depth: -1,
				includeVamm: true,
				numVammOrders: 100,
				updateOnChange: true,
				fallbackL2Generators: [],
			});
		}
		for (const market of spotMarkets) {
			this.marketL2Args.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.SPOT,
				marketName: market.symbol,
				depth: -1,
				includeVamm: false,
				updateOnChange: true,
				fallbackL2Generators: [
					config.spotMarketSubscribers[market.marketIndex].phoenix,
					config.spotMarketSubscribers[market.marketIndex].serum,
				].filter((a) => !!a),
			});
		}
	}

	override async updateDLOB(): Promise<void> {
		await super.updateDLOB();
		for (const l2Args of this.marketL2Args) {
			this.getL2AndSendMsg(l2Args);
		}
	}

	getL2AndSendMsg(l2Args: wsMarketL2Args): void {
		const grouping = l2Args.grouping;
		const { marketName, ...l2FuncArgs } = l2Args;
		const l2 = this.getL2(l2FuncArgs);
		const marketType = isVariant(l2Args.marketType, 'perp') ? 'perp' : 'spot';
		let l2Formatted: any;
		if (grouping) {
			const groupingBN = new BN(grouping);
			l2Formatted = l2WithBNToStrings(groupL2(l2, groupingBN, l2Args.depth));
		} else {
			l2Formatted = l2WithBNToStrings(l2);
		}

		if (l2Args.updateOnChange) {
			if (
				this.lastSeenL2Formatted
					.get(l2Args.marketType)
					?.get(l2Args.marketIndex) === JSON.stringify(l2Formatted)
			)
				return;
		}
		this.lastSeenL2Formatted
			.get(l2Args.marketType)
			?.set(l2Args.marketIndex, JSON.stringify(l2Formatted));
		l2Formatted['marketName'] = marketName?.toUpperCase();
		l2Formatted['marketType'] = marketType?.toLowerCase();
		l2Formatted['marketIndex'] = l2Args.marketIndex;
		addOracletoResponse(
			l2Formatted,
			this.driftClient,
			l2Args.marketType,
			l2Args.marketIndex
		);
		this.redisClient.client.publish(
			`orderbook_${marketType}_${l2Args.marketIndex}`,
			JSON.stringify(l2Formatted)
		);
		this.redisClient.client.set(
			`last_update_orderbook_${marketType}_${l2Args.marketIndex}`,
			JSON.stringify(l2Formatted)
		);
	}
}
