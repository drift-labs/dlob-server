import { AuctionParamArgs } from "./types";

export const MEASURED_ENDPOINTS = [
	'/priorityFees',
	'/batchPriorityFees',
	'/topMakers',
	'/l2',
	'/batchL2',
	'/batchL2Cache',
	'/l3',
];

export const DEFAULT_MARKET_AUCTION_DURATION = 20;
export const DEFAULT_LIMIT_AUCTION_DURATION = 60;
const DEFAULT_AUCTION_END_PRICE_OFFSET = 0.1;
const DEFAULT_AUCTION_END_PRICE_FROM = 'worst';

export const DEFAULT_AUCTION_PARAMS: Partial<AuctionParamArgs> = {
	isOracleOrder: true,
	auctionDuration: DEFAULT_MARKET_AUCTION_DURATION,
	auctionStartPriceOffset: 0,
	auctionEndPriceOffset: DEFAULT_AUCTION_END_PRICE_OFFSET,
	auctionStartPriceOffsetFrom: 'oracle',
	auctionEndPriceOffsetFrom: DEFAULT_AUCTION_END_PRICE_FROM,
	additionalEndPriceBuffer: '0',
};
