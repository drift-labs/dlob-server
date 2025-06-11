import {
	BN,
	BigNum,
	DLOBSubscriber,
	DLOBSubscriptionConfig,
	DriftEnv,
	L2OrderBookGenerator,
	MarketType,
	ONE,
	Order,
	OrderStatus,
	OrderTriggerCondition,
	OrderType,
	PRICE_PRECISION,
	PerpOperation,
	PositionDirection,
	ZERO,
	isOperationPaused,
	isVariant,
} from '@drift-labs/sdk';
import { RedisClient } from '@drift/common/clients';
import { logger } from '../utils/logger';
import {
	SubscriberLookup,
	addMarketSlotToResponse,
	addOracletoResponse,
	l2WithBNToStrings,
	parsePositiveIntArray,
	publishGroupings,
} from '../utils/utils';
import { setHealthStatus, HEALTH_STATUS } from '../core/metrics';

export type wsMarketArgs = {
	marketIndex: number;
	marketType: MarketType;
	marketName: string;
	depth: number;
	includeVamm: boolean;
	numVammOrders?: number;
	fallbackL2Generators?: L2OrderBookGenerator[];
	updateOnChange?: boolean;
	tickSize?: BN;
};

require('dotenv').config();

const PERP_MAKRET_STALENESS_THRESHOLD = 30 * 60 * 1000;
const SPOT_MAKRET_STALENESS_THRESHOLD = 60 * 60 * 1000;
const STALE_ORACLE_REMOVE_VAMM_THRESHOLD = 160;

const INDICATIVE_QUOTES_PUBKEY = 'inDNdu3ML4vG5LNExqcwuCQtLcCU8KfK5YM2qYV3JJz';

const PERP_MARKETS_TO_SKIP_SLOT_CHECK =
	process.env.PERP_MARKETS_TO_SKIP_SLOT_CHECK !== undefined
		? parsePositiveIntArray(process.env.PERP_MARKETS_TO_SKIP_SLOT_CHECK)
		: [];
const SPOT_MARKETS_TO_SKIP_SLOT_CHECK =
	process.env.SPOT_MARKETS_TO_SKIP_SLOT_CHECK !== undefined
		? parsePositiveIntArray(process.env.SPOT_MARKETS_TO_SKIP_SLOT_CHECK)
		: [];

export type wsMarketInfo = {
	marketIndex: number;
	marketName: string;
};

export class DLOBSubscriberIO extends DLOBSubscriber {
	public marketArgs: wsMarketArgs[] = [];
	public lastSeenL2Formatted: Map<MarketType, Map<number, any>>;
	redisClient: RedisClient;
	indicativeQuotesRedisClient?: RedisClient;
	public killSwitchSlotDiffThreshold: number;
	public lastMarketSlotMap: Map<
		MarketType,
		Map<number, { slot: number; ts: number }>
	>;

	constructor(
		config: DLOBSubscriptionConfig & {
			env: DriftEnv;
			redisClient: RedisClient;
			indicativeQuotesRedisClient?: RedisClient;
			perpMarketInfos: wsMarketInfo[];
			spotMarketInfos: wsMarketInfo[];
			spotMarketSubscribers: SubscriberLookup;
			killSwitchSlotDiffThreshold?: number;
		}
	) {
		super(config);
		this.redisClient = config.redisClient;
		this.indicativeQuotesRedisClient = config.indicativeQuotesRedisClient;

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
			const includeVamm = !isOperationPaused(
				perpMarket.pausedOperations,
				PerpOperation.AMM_FILL
			);

			this.marketArgs.push({
				marketIndex: market.marketIndex,
				marketType: MarketType.PERP,
				marketName: market.marketName,
				depth: -1,
				numVammOrders: 100,
				includeVamm,
				updateOnChange: false,
				fallbackL2Generators: [],
				tickSize: perpMarket?.amm?.orderTickSize ?? ONE,
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
					config.spotMarketSubscribers[market.marketIndex].openbook,
				].filter((a) => !!a),
				tickSize:
					config.spotMarketSubscribers[market.marketIndex].tickSize ?? ONE,
			});
		}
	}

	override async updateDLOB(): Promise<void> {
		await super.updateDLOB();
		let indicativeOrderId = 0;
		for (const marketArgs of this.marketArgs) {
			try {
				if (this.indicativeQuotesRedisClient) {
					const marketType = isVariant(marketArgs.marketType, 'perp')
						? 'perp'
						: 'spot';

					const mms = await this.indicativeQuotesRedisClient.smembers(
						`market_mms_${marketType}_${marketArgs.marketIndex}`
					);
					const mmQuotes = await Promise.all(
						mms.map((mm) => {
							return this.indicativeQuotesRedisClient.get(
								`mm_quotes_${marketType}_${marketArgs.marketIndex}_${mm}`
							);
						})
					);

					const nowMinus1000Ms = Date.now() - 1000;
					for (const quote of mmQuotes) {
						if (Number(quote['ts']) > nowMinus1000Ms) {
							const indicativeBaseOrder: Order = {
								status: OrderStatus.OPEN,
								orderType: OrderType.LIMIT,
								orderId: 0,
								slot: new BN(this.slotSource.getSlot()),
								marketIndex: marketArgs.marketIndex,
								marketType: marketArgs.marketType,
								baseAssetAmount: ZERO,
								immediateOrCancel: false,
								direction: PositionDirection.LONG,
								oraclePriceOffset: 0,
								maxTs: new BN(quote['ts'] + 1000),
								reduceOnly: false,
								triggerCondition: OrderTriggerCondition.ABOVE,
								price: ZERO,
								userOrderId: 0,
								postOnly: true,
								auctionDuration: 0,
								auctionStartPrice: ZERO,
								auctionEndPrice: ZERO,
								// Rest are not necessary and set for type conforming
								existingPositionDirection: PositionDirection.LONG,
								triggerPrice: ZERO,
								baseAssetAmountFilled: ZERO,
								quoteAssetAmountFilled: ZERO,
								quoteAssetAmount: ZERO,
								bitFlags: 0,
								postedSlotTail: 0,
							};

							if (quote['bid_size'] && quote['bid_price'] != null) {
								// Sanity check bid price and size

								const indicativeBid: Order = Object.assign(
									{},
									indicativeBaseOrder,
									{
										orderId: indicativeOrderId,
										oraclePriceOffset: quote['is_oracle_offset']
											? quote['bid_price']
											: 0,
										price: quote['is_oracle_offset']
											? 0
											: new BN(quote['bid_price']),
										baseAssetAmount: new BN(quote['bid_size']),
										direction: PositionDirection.LONG,
									}
								);
								this.dlob.insertOrder(
									indicativeBid,
									INDICATIVE_QUOTES_PUBKEY,
									this.slotSource.getSlot(),
									false
								);
								indicativeOrderId += 1;
							}

							if (quote['ask_size'] && quote['ask_price'] != null) {
								const indicativeAsk: Order = Object.assign(
									{},
									indicativeBaseOrder,
									{
										orderId: indicativeOrderId,
										oraclePriceOffset: quote['is_oracle_offset']
											? quote['ask_price']
											: 0,
										price: quote['is_oracle_offset']
											? 0
											: new BN(quote['ask_price']),
										baseAssetAmount: new BN(quote['ask_size']),
										direction: PositionDirection.SHORT,
									}
								);
								this.dlob.insertOrder(
									indicativeAsk,
									INDICATIVE_QUOTES_PUBKEY,
									this.slotSource.getSlot(),
									false
								);
								indicativeOrderId += 1;
							}
						}
					}
				}
				this.getL2AndSendMsg(marketArgs);
				this.getL3AndSendMsg(marketArgs);
			} catch (error) {
				console.error(error);
				console.log(`Error getting L2 ${marketArgs.marketName}`);
			}
		}
	}

	getL2AndSendMsg(marketArgs: wsMarketArgs): void {
		const clientPrefix = this.redisClient.forceGetClient().options.keyPrefix;
		const { marketName, ...l2FuncArgs } = marketArgs;
		const marketType = isVariant(marketArgs.marketType, 'perp')
			? 'perp'
			: 'spot';

		// Test for oracle staleness to know whether to include vamm
		const dlobSlot = this.slotSource.getSlot();
		const oracleData =
			marketType === 'perp'
				? this.driftClient.getOracleDataForPerpMarket(marketArgs.marketIndex)
				: this.driftClient.getOracleDataForSpotMarket(marketArgs.marketIndex);
		const oracleSlot = oracleData.slot;
		const isPerpMarketAndPrelaunchMarket =
			marketType === 'perp' &&
			isVariant(
				this.driftClient.getPerpMarketAccount(marketArgs.marketIndex).amm
					.oracleSource,
				'prelaunch'
			);
		let includeVamm = marketArgs.includeVamm;
		if (
			dlobSlot - oracleSlot.toNumber() > STALE_ORACLE_REMOVE_VAMM_THRESHOLD &&
			!isPerpMarketAndPrelaunchMarket
		) {
			logger.info('Oracle is stale, removing vamm orders');
			includeVamm = false;
		}

		const l2 = this.getL2({ ...l2FuncArgs, includeVamm });
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

		const l2Formatted = l2WithBNToStrings(l2);

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
		const skipSlotCheck =
			(marketType === 'perp' &&
				PERP_MARKETS_TO_SKIP_SLOT_CHECK.includes(marketArgs.marketIndex)) ||
			(marketType === 'spot' &&
				SPOT_MARKETS_TO_SKIP_SLOT_CHECK.includes(marketArgs.marketIndex));
		if (
			Math.abs(slot - parseInt(l2Formatted['oracleData']['slot'])) >
				this.killSwitchSlotDiffThreshold &&
			!skipSlotCheck &&
			!isPerpMarketAndPrelaunchMarket
		) {
			console.log(
				`Unhealthy process due to slot diffs for market ${marketName}. dlobProviderSlot: ${slot}, oracleSlot: ${l2Formatted['oracleData']['slot']}`
			);
			setHealthStatus(HEALTH_STATUS.Restart);
		}

		// Check if times and slots are too different for market
		const MAKRET_STALENESS_THRESHOLD =
			marketType === 'perp'
				? PERP_MAKRET_STALENESS_THRESHOLD
				: SPOT_MAKRET_STALENESS_THRESHOLD;
		if (
			lastMarketSlotAndTime &&
			l2Formatted['marketSlot'] === lastMarketSlotAndTime.slot &&
			Date.now() - lastMarketSlotAndTime.ts > MAKRET_STALENESS_THRESHOLD &&
			!skipSlotCheck
		) {
			logger.warn(
				`Unhealthy process due to same slot for market ${marketName} after > ${MAKRET_STALENESS_THRESHOLD}ms. dlobProviderSlot: ${slot}, marketSlot: ${l2Formatted['marketSlot']}`
			);
			setHealthStatus(HEALTH_STATUS.Restart);
		} else if (
			lastMarketSlotAndTime &&
			l2Formatted['marketSlot'] !== lastMarketSlotAndTime.slot
		) {
			logger.warn(
				`Updating market slot for ${marketArgs.marketName} from ${lastMarketSlotAndTime.slot} -> ${l2Formatted['marketSlot']}`
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

		this.redisClient.publish(
			`${clientPrefix}orderbook_${marketType}_${marketArgs.marketIndex}${
				this.indicativeQuotesRedisClient ? '_indicative' : ''
			}`,
			l2Formatted
		);
		this.redisClient.set(
			`last_update_orderbook_${marketType}_${marketArgs.marketIndex}${
				this.indicativeQuotesRedisClient ? '_indicative' : ''
			}`,
			l2Formatted_depth100
		);

		publishGroupings(
			l2Formatted,
			marketArgs,
			this.redisClient,
			clientPrefix,
			marketType,
			this.indicativeQuotesRedisClient
		);

		if (!this.indicativeQuotesRedisClient) {
			const bids = this.dlob
				.getBestMakers({
					marketIndex: marketArgs.marketIndex,
					marketType: marketArgs.marketType,
					direction: PositionDirection.LONG,
					slot: slot,
					oraclePriceData: oracleData,
					numMakers: 4,
				})
				.map((x) => x.toString());
			const asks = this.dlob
				.getBestMakers({
					marketIndex: marketArgs.marketIndex,
					marketType: marketArgs.marketType,
					direction: PositionDirection.SHORT,
					slot,
					oraclePriceData: oracleData,
					numMakers: 4,
				})
				.map((x) => x.toString());
			this.redisClient.set(
				`last_update_orderbook_best_makers_${marketType}_${marketArgs.marketIndex}`,
				{ bids, asks, slot }
			);
		}
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

		this.redisClient.set(
			`last_update_orderbook_l3_${marketType}_${marketArgs.marketIndex}${
				this.indicativeQuotesRedisClient ? '_indicative' : ''
			}`,
			l3
		);
	}
}
