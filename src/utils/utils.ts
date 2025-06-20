import {
	BN,
	BigNum,
	DriftClient,
	DriftEnv,
	L2OrderBook,
	L3OrderBook,
	MarketType,
	OpenbookV2Subscriber,
	OraclePriceData,
	PhoenixSubscriber,
	PublicKey,
	SerumSubscriber,
	SpotMarketConfig,
	decodeUser,
	isVariant,
	PositionDirection,
	ZERO,
	BASE_PRECISION,
	PRICE_PRECISION,
	calculateEstimatedEntryPriceWithL2,
	AssetType,
	MainnetSpotMarkets,
	DevnetSpotMarkets,
	QUOTE_PRECISION,
} from '@drift-labs/sdk';
import { RedisClient } from '@drift/common/clients';

import { logger } from './logger';
import { NextFunction, Request, Response } from 'express';
import FEATURE_FLAGS from './featureFlags';
import { Connection } from '@solana/web3.js';
import { wsMarketArgs } from 'src/dlob-subscriber/DLOBSubscriberIO';

export const GROUPING_OPTIONS = [1, 10, 100, 500, 1000];
export const GROUPING_DEPENDENCIES = {
	1: null,
	10: 1,
	100: 10,
	500: 100,
	1000: 100,
};
import { DEFAULT_AUCTION_PARAMS } from './constants';
import { AuctionParamArgs } from './types';
import { COMMON_MATH, ENUM_UTILS } from '@drift/common';

export const l2WithBNToStrings = (l2: L2OrderBook): any => {
	for (const key of Object.keys(l2)) {
		for (const idx in l2[key]) {
			const level = l2[key][idx];
			const sources = level['sources'];
			for (const sourceKey of Object.keys(sources)) {
				sources[sourceKey] = sources[sourceKey].toString();
			}
			l2[key][idx] = {
				price: level.price.toString(),
				size: level.size.toString(),
				sources,
			};
		}
	}
	return l2;
};

export const l3WithBNToStrings = (l3: L3OrderBook): any => {
	for (const key of Object.keys(l3)) {
		for (const idx in l3[key]) {
			const level = l3[key][idx];
			l3[key][idx] = {
				price: level.price.toString(),
				size: level.size.toString(),
				maker: level.maker.toBase58(),
				orderId: level.orderId.toString(),
			};
		}
	}
	return l3;
};

export function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export function parsePositiveIntArray(
	intArray: string,
	separator = ','
): number[] {
	return intArray
		.split(separator)
		.map((s) => s.trim())
		.map((s) => parseInt(s))
		.filter((n) => !isNaN(n) && n >= 0);
}

export const getOracleForMarket = (
	driftClient: DriftClient,
	marketType: MarketType,
	marketIndex: number
): number => {
	if (isVariant(marketType, 'spot')) {
		return driftClient.getOracleDataForSpotMarket(marketIndex).price.toNumber();
	} else if (isVariant(marketType, 'perp')) {
		return driftClient.getOracleDataForPerpMarket(marketIndex).price.toNumber();
	}
};

type SerializableOraclePriceData = {
	price: string;
	slot: string;
	confidence: string;
	hasSufficientNumberOfDataPoints: boolean;
	twap?: string;
	twapConfidence?: string;
	maxPrice?: string;
};

const getSerializableOraclePriceData = (
	oraclePriceData: OraclePriceData
): SerializableOraclePriceData => {
	return {
		price: oraclePriceData.price?.toString?.(),
		slot: oraclePriceData.slot?.toString?.(),
		confidence: oraclePriceData.confidence?.toString?.(),
		hasSufficientNumberOfDataPoints:
			oraclePriceData.hasSufficientNumberOfDataPoints,
		twap: oraclePriceData.twap?.toString?.(),
		twapConfidence: oraclePriceData.twapConfidence?.toString?.(),
		maxPrice: oraclePriceData.maxPrice?.toString?.(),
	};
};

export const getOracleDataForMarket = (
	driftClient: DriftClient,
	marketType: MarketType,
	marketIndex: number
): SerializableOraclePriceData => {
	if (isVariant(marketType, 'spot')) {
		return getSerializableOraclePriceData(
			driftClient.getOracleDataForSpotMarket(marketIndex)
		);
	} else if (isVariant(marketType, 'perp')) {
		return getSerializableOraclePriceData(
			driftClient.getOracleDataForPerpMarket(marketIndex)
		);
	}
};

export const addOracletoResponse = (
	response: L2OrderBook | L3OrderBook,
	driftClient: DriftClient,
	marketType: MarketType,
	marketIndex: number
): void => {
	if (FEATURE_FLAGS.OLD_ORACLE_PRICE_IN_L2) {
		response['oracle'] = getOracleForMarket(
			driftClient,
			marketType,
			marketIndex
		);
		if (response['oracle'] == 0) {
			logger.info(`oracle price is 0 for ${marketType}-${marketIndex}`);
		}
	}
	if (FEATURE_FLAGS.NEW_ORACLE_DATA_IN_L2) {
		response['oracleData'] = getOracleDataForMarket(
			driftClient,
			marketType,
			marketIndex
		);
		if (!response['oracleData'].price) {
			logger.info(`oracle price is undefined for ${marketType}-${marketIndex}`);
		}
		if (Number(response['oracleData'].price) == 0) {
			logger.info(`oracle price is 0 for ${marketType}-${marketIndex}`);
		}
	}
};

export const addMarketSlotToResponse = (
	response: L2OrderBook | L3OrderBook,
	driftClient: DriftClient,
	marketType: MarketType,
	marketIndex: number
): void => {
	let marketSlot: number;
	if (isVariant(marketType, 'perp')) {
		marketSlot =
			driftClient.accountSubscriber.getMarketAccountAndSlot(marketIndex).slot;
	} else {
		marketSlot =
			driftClient.accountSubscriber.getSpotMarketAccountAndSlot(
				marketIndex
			).slot;
	}
	response['marketSlot'] = marketSlot;
};

export function aggregatePrices(entries, side, pricePrecision) {
	const isAsk = side === 'ask';
	const result = new Map();

	entries.forEach((entry) => {
		const price = parseFloat(entry.price);
		const data = {
			size: parseFloat(entry.size),
			sources: entry.sources || {},
		};

		let bucketPrice, displayPrice;
		if (isAsk) {
			displayPrice = Math.ceil(price / pricePrecision) * pricePrecision;
			bucketPrice = displayPrice;
		} else {
			displayPrice = Math.floor(price / pricePrecision) * pricePrecision;
			bucketPrice = displayPrice;
		}

		const bucketKey = Math.round(bucketPrice);

		if (!result.has(bucketKey)) {
			result.set(bucketKey, {
				size: 0,
				price: displayPrice,
				sources: {},
			});
		}

		const bucketData = result.get(bucketKey);
		bucketData.size += data.size;

		if (data.sources) {
			Object.entries(data.sources).forEach(
				([sourceKey, sourceSize]: [string, string]) => {
					if (!bucketData.sources[sourceKey]) {
						bucketData.sources[sourceKey] = 0;
					}
					bucketData.sources[sourceKey] += parseFloat(sourceSize);
				}
			);
		}
	});

	return Array.from(result.values());
}

export function publishGroupings(
	l2Formatted,
	marketArgs: wsMarketArgs,
	redisClient: RedisClient,
	clientPrefix: string,
	marketType: string,
	indicativeQuotesRedisClient: RedisClient
) {
	const groupingResults = new Map();

	GROUPING_OPTIONS.forEach((group) => {
		const pricePrecision = BigNum.from(group).mul(marketArgs.tickSize).toNum();
		const dependency = GROUPING_DEPENDENCIES[group];

		let fullAggregatedBids, fullAggregatedAsks;

		if (dependency && groupingResults.has(dependency)) {
			const previousResults = groupingResults.get(dependency);

			fullAggregatedBids = aggregatePrices(
				previousResults.bids,
				'bid',
				pricePrecision
			).sort((a, b) => b[0] - a[0]);

			fullAggregatedAsks = aggregatePrices(
				previousResults.asks,
				'ask',
				pricePrecision
			).sort((a, b) => a[0] - b[0]);
		} else {
			fullAggregatedBids = aggregatePrices(
				l2Formatted.bids,
				'bid',
				pricePrecision
			).sort((a, b) => b[0] - a[0]);

			fullAggregatedAsks = aggregatePrices(
				l2Formatted.asks,
				'ask',
				pricePrecision
			).sort((a, b) => a[0] - b[0]);
		}

		groupingResults.set(group, {
			bids: fullAggregatedBids,
			asks: fullAggregatedAsks,
		});

		const aggregatedBids = fullAggregatedBids.slice(0, 20);
		const aggregatedAsks = fullAggregatedAsks.slice(0, 20);
		const l2Formatted_grouped20 = Object.assign({}, l2Formatted, {
			bids: aggregatedBids,
			asks: aggregatedAsks,
		});

		redisClient.publish(
			`${clientPrefix}orderbook_${marketType}_${
				marketArgs.marketIndex
			}_grouped_${group}${indicativeQuotesRedisClient ? '_indicative' : ''}`,
			l2Formatted_grouped20
		);
	});
}

/**
 * Takes in a req.query like: `{
 * 		marketName: 'SOL-PERP,BTC-PERP,ETH-PERP',
 * 		marketType: undefined,
 * 		marketIndices: undefined,
 * 		...
 * 	}` and returns a normalized object like:
 *
 * `[
 * 		{marketName: 'SOL-PERP', marketType: undefined, marketIndex: undefined,...},
 * 		{marketName: 'BTC-PERP', marketType: undefined, marketIndex: undefined,...},
 * 		{marketName: 'ETH-PERP', marketType: undefined, marketIndex: undefined,...}
 * ]`
 *
 * @param rawParams req.query object
 * @returns normalized query params for batch requests, or undefined if there is a mismatched length
 */
export const normalizeBatchQueryParams = (rawParams: {
	[key: string]: string | undefined;
}): Array<{ [key: string]: string | undefined }> => {
	const normedParams: Array<{ [key: string]: string | undefined }> = [];
	const parsedParams = {};

	// parse the query string into arrays
	for (const key of Object.keys(rawParams)) {
		const rawParam = rawParams[key];
		if (rawParam === undefined) {
			parsedParams[key] = [];
		} else {
			parsedParams[key] = rawParam.split(',') || [rawParam];
		}
	}

	// of all parsedParams, find the max length
	const maxLength = Math.max(
		...Object.values(parsedParams).map((param: Array<unknown>) => param.length)
	);

	// all params have to be either 0 length, or maxLength to be valid
	const values = Object.values(parsedParams);
	const validParams = values.every(
		(value: Array<unknown>) => value.length === 0 || value.length === maxLength
	);
	if (!validParams) {
		return undefined;
	}

	// merge all params into an array of objects
	// normalize all params to the same length, filling in undefineds
	for (let i = 0; i < maxLength; i++) {
		const newParam = {};
		for (const key of Object.keys(parsedParams)) {
			const parsedParam = parsedParams[key];
			newParam[key] =
				parsedParam.length === maxLength ? parsedParam[i] : undefined;
		}
		normedParams.push(newParam);
	}

	return normedParams;
};

export const validateWsSubscribeMsg = (
	msg: any,
	sdkConfig: any
): { valid: boolean; msg?: string } => {
	const maxPerpMarketIndex = Math.max(
		...sdkConfig.PERP_MARKETS.map((m) => m.marketIndex)
	);
	const maxSpotMarketIndex = Math.max(
		...sdkConfig.SPOT_MARKETS.map((m) => m.marketIndex)
	);

	if (msg['marketIndex'] < 0) {
		return { valid: false, msg: `Invalid marketIndex, must be >= 0` };
	}

	if (
		msg['marketType'].toLowerCase() == 'spot' &&
		parseInt(msg['marketIndex']) > maxSpotMarketIndex
	) {
		return {
			valid: false,
			msg: `Invalid marketIndex for marketType: ${msg['marketType']}`,
		};
	}

	if (
		msg['marketType'].toLowerCase() == 'perp' &&
		parseInt(msg['marketIndex']) > maxPerpMarketIndex
	) {
		return {
			valid: false,
			msg: `Invalid marketIndex for marketType: ${msg['marketType']}`,
		};
	}

	if (
		msg['marketType'].toLowerCase() != 'perp' &&
		msg['marketType'] != 'spot'
	) {
		return {
			valid: false,
			msg: `Invalid marketType: ${msg['marketType']}`,
		};
	}

	return { valid: true };
};

export const validateDlobQuery = (
	driftClient: DriftClient,
	driftEnv: DriftEnv,
	marketType?: string,
	marketIndex?: string,
	marketName?: string
): {
	normedMarketType?: MarketType;
	normedMarketIndex?: number;
	error?: string;
} => {
	let normedMarketType: MarketType = undefined;
	let normedMarketIndex: number = undefined;
	let normedMarketName: string = undefined;
	if (marketName === undefined) {
		if (marketIndex === undefined || marketType === undefined) {
			return {
				error:
					'Bad Request: (marketName) or (marketIndex and marketType) must be supplied',
			};
		}

		// validate marketType
		switch ((marketType as string).toLowerCase()) {
			case 'spot': {
				normedMarketType = MarketType.SPOT;
				normedMarketIndex = parseInt(marketIndex as string);
				const spotMarketIndicies = driftClient
					.getSpotMarketAccounts()
					.map((mkt) => mkt.marketIndex);
				if (!spotMarketIndicies.includes(normedMarketIndex)) {
					return {
						error: 'Bad Request: invalid marketIndex',
					};
				}
				break;
			}
			case 'perp': {
				normedMarketType = MarketType.PERP;
				normedMarketIndex = parseInt(marketIndex as string);
				const perpMarketIndicies = driftClient
					.getPerpMarketAccounts()
					.map((mkt) => mkt.marketIndex);
				if (!perpMarketIndicies.includes(normedMarketIndex)) {
					return {
						error: 'Bad Request: invalid marketIndex',
					};
				}
				break;
			}
			default:
				return {
					error: 'Bad Request: marketType must be either "spot" or "perp"',
				};
		}
	} else {
		// validate marketName
		normedMarketName = (marketName as string).toUpperCase();
		const derivedMarketInfo =
			driftClient.getMarketIndexAndType(normedMarketName);
		if (!derivedMarketInfo) {
			return {
				error: 'Bad Request: unrecognized marketName',
			};
		}
		normedMarketType = derivedMarketInfo.marketType;
		normedMarketIndex = derivedMarketInfo.marketIndex;
	}

	return {
		normedMarketType,
		normedMarketIndex,
	};
};

export const getAccountFromId = async (
	userMapClient: RedisClient,
	topMakers: string[]
) => {
	return Promise.all(
		topMakers.map(async (userAccountPubKey) => {
			const userAccountEncoded = await userMapClient.getRaw(userAccountPubKey);
			if (userAccountEncoded) {
				return {
					userAccountPubKey,
					account: decodeUser(
						Buffer.from(userAccountEncoded.split('::')[1], 'base64')
					),
				};
			}
			return {
				userAccountPubKey,
				account: null,
			};
		})
	).then((results) => results.filter((user) => !!user));
};

export const getRawAccountFromId = async (
	userMapClient: RedisClient,
	topMakers: string[],
	connection: Connection
): Promise<
	{
		userAccountPubKey: string;
		accountBase64: string;
	}[]
> => {
	return Promise.all(
		topMakers.map(async (userAccountPubKey) => {
			const userAccountEncoded = await userMapClient.getRaw(userAccountPubKey);
			if (userAccountEncoded) {
				return {
					userAccountPubKey,
					accountBase64: userAccountEncoded.split('::')[1],
				};
			} else {
				// user is not in the userMap, try to fetch from the connection
				const account = await connection.getAccountInfo(
					new PublicKey(userAccountPubKey)
				);
				if (account) {
					return {
						userAccountPubKey,
						accountBase64: account.data.toString('base64'),
					};
				}
			}

			return {
				userAccountPubKey,
				accountBase64: null,
			};
		})
	).then((results) => results.filter((user) => !!user));
};

export function errorHandler(
	err: Error,
	_req: Request,
	res: Response,
	_next: NextFunction
): void {
	logger.error(`errorHandler, message: ${err.message}, stack: ${err.stack}`);
	if (!res.headersSent) {
		res.status(500).send('Internal error');
	}
}

/**
 * Spot market utils
 */

export const getPhoenixSubscriber = (
	driftClient: DriftClient,
	marketConfig: SpotMarketConfig,
	sdkConfig
): PhoenixSubscriber => {
	return new PhoenixSubscriber({
		connection: driftClient.connection,
		programId: new PublicKey(sdkConfig.PHOENIX),
		marketAddress: marketConfig.phoenixMarket,
		accountSubscription: {
			type: 'websocket',
		},
	});
};

export const getSerumSubscriber = (
	driftClient: DriftClient,
	marketConfig: SpotMarketConfig,
	sdkConfig
): SerumSubscriber => {
	return new SerumSubscriber({
		connection: driftClient.connection,
		programId: new PublicKey(sdkConfig.SERUM_V3),
		marketAddress: marketConfig.serumMarket,
		accountSubscription: {
			type: 'websocket',
		},
	});
};

export const getOpenbookSubscriber = (
	driftClient: DriftClient,
	marketConfig: SpotMarketConfig,
	sdkConfig
): OpenbookV2Subscriber => {
	return new OpenbookV2Subscriber({
		connection: driftClient.connection,
		programId: new PublicKey(sdkConfig.OPENBOOK),
		marketAddress: marketConfig.openbookMarket,
		accountSubscription: {
			type: 'websocket',
		},
	});
};

export type SubscriberLookup = {
	[marketIndex: number]: {
		phoenix?: PhoenixSubscriber;
		serum?: SerumSubscriber;
		openbook?: OpenbookV2Subscriber;
		tickSize?: BN;
	};
};

export const selectMostRecentBySlot = (
	responses: any[]
): {
	slot: number;
	[key: string]: any;
} => {
	const parsedResponses = responses
		.map((response) => {
			try {
				return JSON.parse(response);
			} catch {
				return null;
			}
		})
		.filter((parsed) => parsed && typeof parsed.slot === 'number');
	return parsedResponses.reduce((mostRecent, current) => {
		return !mostRecent || current.slot > mostRecent.slot ? current : mostRecent;
	}, null);
};

export function createMarketBasedAuctionParams(
	args: AuctionParamArgs,
	overrideDefaults?: Partial<AuctionParamArgs>
): AuctionParamArgs {
	// Determine if this is a major market (PERP with marketIndex 0, 1, or 2)
	const isMajorMarket =
		args.marketType?.toLowerCase() === 'perp' &&
		[0, 1, 2].includes(args.marketIndex);

	// Resolve "marketBased" values
	const resolvedAuctionStartPriceOffsetFrom =
		args.auctionStartPriceOffsetFrom === 'marketBased'
			? isMajorMarket
				? 'mark'
				: 'bestOffer'
			: args.auctionStartPriceOffsetFrom;

	const resolvedAuctionStartPriceOffset =
		args.auctionStartPriceOffset === 'marketBased'
			? isMajorMarket
				? 0
				: -0.1
			: args.auctionStartPriceOffset;

	// Set market-specific defaults (only used if values are undefined)
	const marketSpecificDefaults: Partial<AuctionParamArgs> = {
		...DEFAULT_AUCTION_PARAMS,
		auctionStartPriceOffsetFrom: isMajorMarket ? 'mark' : 'bestOffer',
		auctionStartPriceOffset: isMajorMarket ? 0 : -0.1,
	};

	// Apply custom overrides if provided
	const finalDefaults = overrideDefaults
		? { ...marketSpecificDefaults, ...overrideDefaults }
		: marketSpecificDefaults;

	return {
		...finalDefaults,
		...args,
		// Override with resolved "marketBased" values if they were provided
		auctionStartPriceOffsetFrom:
			resolvedAuctionStartPriceOffsetFrom ??
			finalDefaults.auctionStartPriceOffsetFrom,
		auctionStartPriceOffset:
			resolvedAuctionStartPriceOffset ?? finalDefaults.auctionStartPriceOffset,
	};
}

/**
 * Parse boolean values from string query parameters
 * @param value - string value from query parameter
 * @returns boolean | undefined - true for 'true'/'1', false for other values, undefined if input is undefined
 */
export const parseBoolean = (
	value: string | undefined
): boolean | undefined => {
	if (value === undefined) return undefined;
	return value === 'true' || value === '1';
};

/**
 * Safely parse numeric values from string query parameters
 * @param value - string value from query parameter
 * @returns number | undefined - parsed number or undefined if invalid/empty
 */
export const parseNumber = (value: string | undefined): number | undefined => {
	if (!value) return undefined;
	const parsed = parseFloat(value);
	return isNaN(parsed) ? undefined : parsed;
};

/**
 * Convert string to BN with specified precision
 * @param value - string value to convert
 * @param precision - BN precision constant (e.g., BASE_PRECISION, PRICE_PRECISION)
 * @returns BN with proper precision applied
 */
export const stringToBN = (value: string, precision: BN): BN => {
	return new BN(Math.round(parseFloat(value) * precision.toNumber()));
};

/**
 * Convert raw Redis L2 data (with string prices/sizes) to proper L2OrderBook format (with BN values)
 * @param rawL2 - Raw L2 data from Redis with string values
 * @returns L2OrderBook with proper BN values
 */
export const convertRawL2ToBN = (rawL2: any): L2OrderBook => {
	const convertLevel = (level: any) => ({
		...level,
		price: new BN(level.price),
		size: new BN(level.size),
	});

	return {
		...rawL2,
		bids: rawL2.bids?.map(convertLevel) || [],
		asks: rawL2.asks?.map(convertLevel) || [],
	};
};

/**
 * Get L2 orderbook data and calculate estimated prices
 * @param driftClient - DriftClient instance
 * @param marketType - MarketType enum
 * @param marketIndex - Market index number
 * @param direction - Position direction
 * @param amount - Amount as BN (could be base or quote amount)
 * @param assetType - Whether amount is 'base' or 'quote'
 * @param fetchFromRedis - Redis fetch function
 * @param selectMostRecentBySlot - Slot selection function
 * @returns Price data object with oracle, best, entry, worst, and mark prices
 */
export const getEstimatedPrices = async (
	driftClient: DriftClient,
	marketType: MarketType,
	marketIndex: number,
	direction: PositionDirection,
	amount: BN,
	assetType: AssetType,
	fetchFromRedis: (
		key: string,
		selectionCriteria: (responses: any) => any
	) => Promise<any>,
	selectMostRecentBySlot: (responses: any[]) => any
) => {
	const isSpot = isVariant(marketType, 'spot');

	// Get L2 orderbook data using the new utility function
	const redisL2 = await fetchL2FromRedis(
		fetchFromRedis,
		selectMostRecentBySlot,
		marketType,
		marketIndex
	);

	let l2Formatted: L2OrderBook;
	if (redisL2) {
		l2Formatted = convertRawL2ToBN(redisL2);
	} else {
		l2Formatted = {
			bids: [],
			asks: [],
		};
	}

	const oracleData = isSpot
		? driftClient.getOracleDataForSpotMarket(marketIndex)
		: driftClient.getOracleDataForPerpMarket(marketIndex);

	// Get oracle price
	const oraclePrice = new BN(oracleData?.price || 0).mul(PRICE_PRECISION);

	const spreadInfo = COMMON_MATH.calculateSpreadBidAskMark(
		l2Formatted,
		oraclePrice
	);

	const markPrice = spreadInfo?.markPrice ?? oraclePrice;

	// If we have L2 data, calculate estimated prices
	if (l2Formatted.bids?.length > 0 || l2Formatted.asks?.length > 0) {
		try {
			const basePrecision = !isSpot
				? BASE_PRECISION
				: MainnetSpotMarkets[marketIndex].precision;

			const priceEstimate = calculateEstimatedEntryPriceWithL2(
				assetType,
				amount,
				direction,
				basePrecision,
				l2Formatted as L2OrderBook
			);

			return {
				oraclePrice,
				bestPrice: priceEstimate.bestPrice,
				entryPrice: priceEstimate.entryPrice,
				worstPrice: priceEstimate.worstPrice,
				markPrice,
			};
		} catch (error) {
			// If calculation fails, fallback to oracle prices
			console.warn('Price calculation failed, using oracle fallback:', error);
		}
	}

	// Fallback to oracle prices if no L2 data or calculation fails
	return {
		oraclePrice,
		bestPrice: oraclePrice,
		entryPrice: oraclePrice,
		worstPrice: oraclePrice,
		markPrice,
	};
};

/**
 * Maps AuctionParamArgs to the format expected by deriveMarketOrderParams
 * @param params - AuctionParamArgs from the API request
 * @param driftClient - DriftClient instance (optional, for price calculation)
 * @param fetchFromRedis - Redis fetch function (optional, for price calculation)
 * @param selectMostRecentBySlot - Slot selection function (optional, for price calculation)
 * @returns Object formatted for deriveMarketOrderParams function
 */
export const mapToMarketOrderParams = async (
	params: AuctionParamArgs,
	driftClient?: DriftClient,
	fetchFromRedis?: (
		key: string,
		selectionCriteria: (responses: any) => any
	) => Promise<any>,
	selectMostRecentBySlot?: (responses: any[]) => any
) => {
	// Convert marketType string to MarketType enum
	const marketType =
		params.marketType.toLowerCase() === 'spot'
			? MarketType.SPOT
			: MarketType.PERP;

	// Convert direction string to PositionDirection enum
	const direction =
		params.direction === 'long'
			? PositionDirection.LONG
			: PositionDirection.SHORT;

	// Convert amount string to BN based on assetType
	// For base assets, use BASE_PRECISION (1e9)
	// For quote assets, use QUOTE_PRECISION (1e6, typical for USDC)
	const amount =
		params.assetType === 'base'
			? stringToBN(params.amount, BASE_PRECISION)
			: stringToBN(params.amount, QUOTE_PRECISION);

	// Convert additionalEndPriceBuffer string to BN with PRICE_PRECISION (1e6) if provided
	const additionalEndPriceBuffer = params.additionalEndPriceBuffer
		? stringToBN(params.additionalEndPriceBuffer, PRICE_PRECISION)
		: undefined;

	// Calculate estimated prices
	let estimatedPrices;
	if (driftClient && fetchFromRedis && selectMostRecentBySlot) {
		estimatedPrices = await getEstimatedPrices(
			driftClient,
			marketType,
			params.marketIndex,
			direction,
			amount,
			params.assetType,
			fetchFromRedis,
			selectMostRecentBySlot
		);
	} else {
		// Fallback to zero prices if dependencies not provided
		estimatedPrices = {
			oraclePrice: ZERO,
			bestPrice: ZERO,
			entryPrice: ZERO,
			worstPrice: ZERO,
			markPrice: ZERO,
		};
	}

	// Calculate baseAmount based on assetType
	let baseAmount: BN;
	if (params.assetType === 'base') {
		// If assetType is base, use the amount directly
		baseAmount = amount;
	} else {
		// If assetType is quote, convert quote amount to base amount using entry price
		// baseAmount = (quoteAmount * PRICE_PRECISION) / entryPrice
		if (estimatedPrices.entryPrice.gt(ZERO)) {
			baseAmount = amount.mul(PRICE_PRECISION).div(estimatedPrices.entryPrice);
		} else {
			// Fallback to zero if entry price is zero or invalid
			baseAmount = ZERO;
		}
	}

	return {
		marketType,
		marketIndex: params.marketIndex,
		direction,
		maxLeverageSelected: false,
		maxLeverageOrderSize: ZERO,
		baseAmount,
		reduceOnly: params.reduceOnly ?? false,
		allowInfSlippage: params.allowInfSlippage ?? false,
		oraclePrice: estimatedPrices.oraclePrice,
		bestPrice: estimatedPrices.bestPrice,
		entryPrice: estimatedPrices.entryPrice,
		worstPrice: estimatedPrices.worstPrice,
		markPrice: estimatedPrices.markPrice,
		auctionDuration: params.auctionDuration,
		auctionStartPriceOffset: params.auctionStartPriceOffset as number,
		auctionEndPriceOffset: params.auctionEndPriceOffset,
		auctionStartPriceOffsetFrom: params.auctionStartPriceOffsetFrom as any,
		auctionEndPriceOffsetFrom: params.auctionEndPriceOffsetFrom,
		slippageTolerance: params.slippageTolerance,
		isOracleOrder: params.isOracleOrder,
		additionalEndPriceBuffer,
		forceUpToSlippage: true,
		userOrderId: params.userOrderId,
	};
};

/**
 * Format auction parameters for API response
 * @param auctionParams - Raw auction parameters from deriveMarketOrderParams
 * @returns Formatted auction parameters with BNs as strings and enums as readable strings
 */
export const formatAuctionParamsForResponse = (auctionParams: any) => {
	const formatted = { ...auctionParams };

	// we don't use this field anymore, TODO to remove from ui
	delete formatted.constrainedBySlippage;

	// Convert all properties
	Object.keys(formatted).forEach((key) => {
		const value = formatted[key];

		// Check if it's a BN using BN.isBN()
		if (BN.isBN(value)) {
			formatted[key] = value.toString();
		}
		// Check if it's an enum (has nested object structure like {oracle: {}})
		else if (
			value &&
			typeof value === 'object' &&
			Object.keys(value).length === 1
		) {
			try {
				formatted[key] = ENUM_UTILS.toStr(value);
			} catch (e) {
				// If ENUM_UTILS.toStr fails, keep original value
				formatted[key] = value;
			}
		}
	});

	return formatted;
};

/**
 * Fetch L2 orderbook data from Redis
 * @param fetchFromRedis - Redis fetch function
 * @param selectMostRecentBySlot - Slot selection function
 * @param marketType - MarketType enum (spot or perp)
 * @param marketIndex - Market index number
 * @param includeIndicative - Whether to include indicative orders (optional)
 * @returns Promise<any> - Raw L2 data from Redis or null if not found
 */
export const fetchL2FromRedis = async (
	fetchFromRedis: (
		key: string,
		selectionCriteria: (responses: any) => any
	) => Promise<any>,
	selectMostRecentBySlot: (responses: any[]) => any,
	marketType: MarketType,
	marketIndex: number,
	includeIndicative?: boolean
): Promise<any> => {
	const isSpot = isVariant(marketType, 'spot');
	const marketTypeStr = isSpot ? 'spot' : 'perp';
	const indicativeSuffix = includeIndicative ? '_indicative' : '';

	return await fetchFromRedis(
		`last_update_orderbook_${marketTypeStr}_${marketIndex}${indicativeSuffix}`,
		selectMostRecentBySlot
	);
};
