import {
	BN,
	DLOBSubscriber,
	DLOBSubscriptionConfig,
	L2OrderBookGenerator,
	MarketType,
	groupL2,
} from '@drift-labs/sdk';
import { l2WithBNToStrings } from '../utils';
import { Server } from 'socket.io';
import { getIOServer } from '..';

type wsMarketL2Args = {
	marketIndex: number;
	marketType: MarketType;
	marketName: string;
	depth: number;
	includeVamm: boolean;
	grouping?: number;
	fallbackL2Generators?: L2OrderBookGenerator[];
};

export class DLOBSubscriberIO extends DLOBSubscriber {
	public marketL2Args: wsMarketL2Args[] = [];
	io: Server;

	constructor(config: DLOBSubscriptionConfig) {
		super(config);
	}

	override async updateDLOB(): Promise<void> {
		this.io = getIOServer();
		await super.updateDLOB();
		for (const l2Args of this.marketL2Args) {
			this.getL2AndSendMsg(l2Args);
		}
	}

	getL2AndSendMsg(l2Args: wsMarketL2Args): void {
		console.time('getL2AndSendMsg');
		const grouping = l2Args.grouping;
		const l2 = this.getL2(l2Args);
		let l2Formatted: any;
		if (grouping) {
			const groupingBN = new BN(grouping);
			l2Formatted = l2WithBNToStrings(groupL2(l2, groupingBN, l2Args.depth));
		} else {
			l2Formatted = l2WithBNToStrings(l2);
		}
		l2Formatted['marketName'] = l2Args.marketName;
		l2Formatted['marketType'] = l2Args.marketType;
		l2Formatted['marketIndex'] = l2Args.marketIndex;
		this.io.emit('l2Update', l2Formatted);
		console.timeEnd('getL2AndSendMsg');
	}
}
