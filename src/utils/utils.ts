import {
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
	initialize,
	BulkAccountLoader,
} from '@drift-labs/sdk';
import { RedisClient } from '@drift/common/clients';

import { logger } from './logger';
import { NextFunction, Request, Response } from 'express';
import FEATURE_FLAGS from './featureFlags';
import { Commitment, Connection } from '@solana/web3.js';

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

export const initializeAllMarketSubscribers = async (
	driftClient: DriftClient
) => {
	const markets: SubscriberLookup = {};
	const stateCommitment: Commitment = 'confirmed';
	const driftEnv = (process.env.ENV || 'mainnet-beta') as DriftEnv;
	const sdkConfig = initialize({ env: driftEnv });

	for (const market of driftClient.getSpotMarketAccounts()) {
		markets[market.marketIndex] = {
			phoenix: undefined,
		};
		const marketConfig = sdkConfig.SPOT_MARKETS[market.marketIndex];
		if (marketConfig.phoenixMarket) {
			const phoenixConfigAccount =
				await driftClient.getPhoenixV1FulfillmentConfig(
					marketConfig.phoenixMarket
				);
			if (isVariant(phoenixConfigAccount.status, 'enabled')) {
				logger.info(
					`Loading phoenix subscriber for spot market ${market.marketIndex}`
				);
				const bulkAccountLoader = new BulkAccountLoader(
					driftClient.connection,
					stateCommitment,
					2_000
				);
				const phoenixSubscriber = new PhoenixSubscriber({
					connection: driftClient.connection,
					programId: new PublicKey(sdkConfig.PHOENIX),
					marketAddress: phoenixConfigAccount.phoenixMarket,
					accountSubscription: {
						type: 'polling',
						accountLoader: bulkAccountLoader,
					},
				});
				await phoenixSubscriber.subscribe();
				// Test get L2 to know if we should add
				try {
					phoenixSubscriber.getL2Asks();
					phoenixSubscriber.getL2Bids();
					markets[market.marketIndex].phoenix = phoenixSubscriber;
				} catch (e) {
					logger.info(
						`Excluding phoenix for ${market.marketIndex}, error: ${e}`
					);
				}
			}
		}

		if (marketConfig.openbookMarket) {
			const openbookMarketAccount =
				await driftClient.getOpenbookV2FulfillmentConfig(
					marketConfig.openbookMarket
				);

			if (isVariant(openbookMarketAccount.status, 'enabled')) {
				logger.info(
					`Loading openbook subscriber for spot market ${market.marketIndex}`
				);
				const openbookSubscriber = getOpenbookSubscriber(
					driftClient,
					marketConfig,
					sdkConfig
				);
				await openbookSubscriber.subscribe();
				try {
					openbookSubscriber.getL2Asks();
					openbookSubscriber.getL2Bids();
					markets[market.marketIndex].openbook = openbookSubscriber;
				} catch (e) {
					logger.info(
						`Excluding openbook for ${market.marketIndex}, error: ${e}`
					);
				}
			}
		}
	}

	return markets;
};
