import {
	BN,
	DLOBSubscriber,
	DLOBSubscriptionConfig,
	L2OrderBookGenerator,
	MainnetPerpMarkets,
	MainnetSpotMarkets,
	MarketType,
	groupL2,
} from '@drift-labs/sdk';
import { getOracleForMarket, l2WithBNToStrings } from '../utils/utils';
import { Server } from 'socket.io';
import { getIOServer } from '..';

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
	io: Server;

	constructor(config: DLOBSubscriptionConfig) {
		super(config);

		// Set up appropriate maps
		this.lastSeenL2Formatted = new Map();
		this.lastSeenL2Formatted.set(MarketType.SPOT, new Map());
		this.lastSeenL2Formatted.set(MarketType.PERP, new Map());

		// Add all active markets to the market L2Args
		for (const market of MainnetPerpMarkets) {
			this.marketL2Args.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.PERP,
				marketName: market.symbol,
				depth: 2,
				includeVamm: true,
				numVammOrders: 100,
				updateOnChange: true,
				fallbackL2Generators: [],
			});
		}
		for (const market of MainnetSpotMarkets) {
			this.marketL2Args.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.SPOT,
				marketName: market.symbol,
				depth: 2,
				includeVamm: false,
				updateOnChange: true,
				fallbackL2Generators: [],
			});
		}
	}

	override async updateDLOB(): Promise<void> {
		this.io = getIOServer();
		await super.updateDLOB();
		for (const l2Args of this.marketL2Args) {
			this.getL2AndSendMsg(l2Args);
		}
	}

	getL2AndSendMsg(l2Args: wsMarketL2Args): void {
		const grouping = l2Args.grouping;
		const l2 = this.getL2(l2Args);
		let l2Formatted: any;
		if (grouping) {
			const groupingBN = new BN(grouping);
			l2Formatted = l2WithBNToStrings(groupL2(l2, groupingBN, l2Args.depth));
		} else {
			l2Formatted = l2WithBNToStrings(l2);
		}

		if (
			l2Args.updateOnChange &&
			this.lastSeenL2Formatted
				.get(l2Args.marketType)
				?.get(l2Args.marketIndex) === JSON.stringify(l2Formatted)
		) {
			return;
		}
		this.lastSeenL2Formatted
			.get(l2Args.marketType)
			?.set(l2Args.marketIndex, JSON.stringify(l2Formatted));
		l2Formatted['marketName'] = l2Args.marketName;
		l2Formatted['marketType'] = l2Args.marketType;
		l2Formatted['marketIndex'] = l2Args.marketIndex;
		l2Formatted['oracle'] = getOracleForMarket(
			this.driftClient,
			l2Args.marketType,
			l2Args.marketIndex
		);
		this.io.emit('l2Update', l2Formatted);
	}
}
