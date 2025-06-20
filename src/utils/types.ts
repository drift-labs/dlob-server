import { AssetType, MarketTypeStr } from "@drift-labs/sdk";
import { TradeOffsetPrice } from "@drift/common";

export type AuctionParamArgs = {
	// mandatory args
	marketIndex: number;
	marketType: MarketTypeStr;
	direction: 'long' | 'short';
	amount: string;
	assetType: AssetType;

	// optional settings args
	reduceOnly?: boolean;
	allowInfSlippage?: boolean;
	slippageTolerance?: number;
	isOracleOrder?: boolean;
	auctionDuration?: number;
	auctionStartPriceOffset?: number | 'marketBased';
	auctionEndPriceOffset?: number;
	auctionStartPriceOffsetFrom?: TradeOffsetPrice | 'marketBased';
	auctionEndPriceOffsetFrom?: TradeOffsetPrice;
	additionalEndPriceBuffer?: string;
	userOrderId?: number;
};