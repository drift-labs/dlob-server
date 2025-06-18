import { MarketTypeStr } from "@drift-labs/sdk";
import { TradeOffsetPrice } from "@drift/common";

export type AuctionParamArgs = {
	// mandatory args
	marketIndex: number;
	marketType: MarketTypeStr;
	direction: 'long' | 'short';
	baseSize: string;

	// optional settings args
	reduceOnly?: boolean;
	allowInfSlippage?: boolean;
	slippageTolerance?: number;
	isOracleOrder?: boolean;
	auctionDuration?: number;
	auctionStartPriceOffset?: number;
	auctionEndPriceOffset?: number;
	auctionStartPriceOffsetFrom?: TradeOffsetPrice;
	auctionEndPriceOffsetFrom?: TradeOffsetPrice;
	additionalEndPriceBuffer?: string;
};