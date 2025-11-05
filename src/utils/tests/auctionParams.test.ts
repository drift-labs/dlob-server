import { describe, it, expect, jest, beforeEach } from '@jest/globals';
import {
	BN,
	PositionDirection,
	MarketType,
	ZERO,
	QUOTE_PRECISION,
	PRICE_PRECISION,
	BASE_PRECISION,
} from '@drift-labs/sdk';
import {
	createMarketBasedAuctionParams,
	mapToMarketOrderParams,
	formatAuctionParamsForResponse,
	calculateDynamicSlippage,
} from '../utils';

describe('Auction Parameters Functions', () => {
	describe('createMarketBasedAuctionParams', () => {
		it('should apply market-based logic for major PERP markets (0, 1, 2)', () => {
			[0, 1, 2].forEach((marketIndex) => {
				const args = {
					marketIndex,
					marketType: 'perp' as any,
					direction: 'long' as any,
					amount: '100',
					assetType: 'base' as any,
					auctionStartPriceOffsetFrom: 'marketBased' as any,
					auctionStartPriceOffset: 'marketBased' as any,
				};

				const result = createMarketBasedAuctionParams(args);

				expect(result.auctionStartPriceOffsetFrom).toBe('mark');
				expect(result.auctionStartPriceOffset).toBe(0);
			});
		});

		it('should apply market-based logic for minor PERP markets (>2)', () => {
			[3, 4, 5, 10].forEach((marketIndex) => {
				const args = {
					marketIndex,
					marketType: 'perp' as any,
					direction: 'long' as any,
					amount: '100',
					assetType: 'base' as any,
					auctionStartPriceOffsetFrom: 'marketBased' as any,
					auctionStartPriceOffset: 'marketBased' as any,
				};

				const result = createMarketBasedAuctionParams(args);

				expect(result.auctionStartPriceOffsetFrom).toBe('bestOffer');
				expect(result.auctionStartPriceOffset).toBe(-0.1);
			});
		});

		it('should apply market-based logic for SPOT markets when "marketBased" is passed', () => {
			[0, 1, 2, 5, 10].forEach((marketIndex) => {
				const args = {
					marketIndex,
					marketType: 'spot' as any,
					direction: 'long' as any,
					amount: '100',
					assetType: 'base' as any,
					auctionStartPriceOffsetFrom: 'marketBased' as any,
					auctionStartPriceOffset: 'marketBased' as any,
				};

				const result = createMarketBasedAuctionParams(args);

				expect(result.auctionStartPriceOffsetFrom).toBe('bestOffer');
				expect(result.auctionStartPriceOffset).toBe(-0.1);
			});
		});

		it('should treat undefined values the same as "marketBased"', () => {
			const majorMarketArgs = {
				marketIndex: 0,
				marketType: 'perp' as any,
				direction: 'long' as any,
				amount: '100',
				assetType: 'base' as any,
				// auctionStartPriceOffsetFrom and auctionStartPriceOffset are undefined
			};

			const minorMarketArgs = {
				marketIndex: 5,
				marketType: 'perp' as any,
				direction: 'long' as any,
				amount: '100',
				assetType: 'base' as any,
				// auctionStartPriceOffsetFrom and auctionStartPriceOffset are undefined
			};

			const majorResult = createMarketBasedAuctionParams(majorMarketArgs);
			const minorResult = createMarketBasedAuctionParams(minorMarketArgs);

			// Major market behavior
			expect(majorResult.auctionStartPriceOffsetFrom).toBe('mark');
			expect(majorResult.auctionStartPriceOffset).toBe(0);

			// Minor market behavior
			expect(minorResult.auctionStartPriceOffsetFrom).toBe('bestOffer');
			expect(minorResult.auctionStartPriceOffset).toBe(-0.1);
		});

		it('should preserve explicit values over market-based defaults', () => {
			const args = {
				marketIndex: 0, // Major market
				marketType: 'perp' as any,
				direction: 'long' as any,
				amount: '100',
				assetType: 'base' as any,
				auctionStartPriceOffsetFrom: 'oracle' as any,
				auctionStartPriceOffset: 0.5,
				auctionEndPriceOffset: 0.2,
				auctionDuration: 45,
			};

			const result = createMarketBasedAuctionParams(args);

			// Should use explicit values, not market-based defaults
			expect(result.auctionStartPriceOffsetFrom).toBe('oracle');
			expect(result.auctionStartPriceOffset).toBe(0.5);
			expect(result.auctionEndPriceOffset).toBe(0.2);
			expect(result.auctionDuration).toBe(45);
		});

		it('should handle mixed explicit and market-based values', () => {
			const args = {
				marketIndex: 0,
				marketType: 'perp' as any,
				direction: 'long' as any,
				amount: '100',
				assetType: 'base' as any,
				auctionStartPriceOffsetFrom: 'oracle' as any, // explicit
				auctionStartPriceOffset: 'marketBased' as any, // market-based
				auctionDuration: 45, // explicit
			};

			const result = createMarketBasedAuctionParams(args);

			expect(result.auctionStartPriceOffsetFrom).toBe('oracle'); // explicit value
			expect(result.auctionStartPriceOffset).toBe(0); // market-based for major market
			expect(result.auctionDuration).toBe(45); // explicit value
		});

		it('should include all original parameters in the result', () => {
			const args = {
				marketIndex: 3,
				marketType: 'perp' as any,
				direction: 'short' as any,
				amount: '250',
				assetType: 'quote' as any,
				reduceOnly: true,
				allowInfSlippage: false,
				slippageTolerance: 0.02,
				userOrderId: 12345,
				auctionStartPriceOffsetFrom: 'marketBased' as any,
				auctionStartPriceOffset: 'marketBased' as any,
			};

			const result = createMarketBasedAuctionParams(args);

			// Should preserve all original parameters
			expect(result.marketIndex).toBe(3);
			expect(result.marketType).toBe('perp');
			expect(result.direction).toBe('short');
			expect(result.amount).toBe('250');
			expect(result.assetType).toBe('quote');
			expect(result.reduceOnly).toBe(true);
			expect(result.allowInfSlippage).toBe(false);
			expect(result.slippageTolerance).toBe(0.02);
			expect(result.userOrderId).toBe(12345);

			// Should apply market-based logic
			expect(result.auctionStartPriceOffsetFrom).toBe('bestOffer');
			expect(result.auctionStartPriceOffset).toBe(-0.1);
		});
	});

	describe('mapToMarketOrderParams with Mock L2 Data', () => {
		const mockDriftClient = {
			getOracleDataForPerpMarket: jest.fn(),
			getOracleDataForSpotMarket: jest.fn(),
		};

		const mockFetchFromRedis = jest.fn() as any;
		const mockSelectMostRecentBySlot = jest.fn() as any;

		const validParams = {
			marketIndex: 0,
			marketType: 'perp' as any,
			direction: 'long' as any,
			amount: new BN(100).mul(BASE_PRECISION).toString(), // 100 in BASE_PRECISION
			assetType: 'base' as any,
			auctionDuration: 30,
			auctionStartPriceOffset: 0,
			auctionEndPriceOffset: 0.1,
			auctionStartPriceOffsetFrom: 'mark' as any,
			auctionEndPriceOffsetFrom: 'mark' as any,
		};

		beforeEach(() => {
			jest.clearAllMocks();
		});

		it('should successfully calculate prices with mock L2 orderbook data', async () => {
			const solPrice = 160; // $160 SOL price

			mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
				price: new BN(solPrice).mul(PRICE_PRECISION),
			});

			// Mock realistic L2 orderbook data
			mockFetchFromRedis.mockImplementation(async (key: string) => {
				if (key.includes('dlob_orderbook')) {
					return {
						bids: [
							{ price: '159950000', size: '1000000000' }, // $159.95, 1 SOL
							{ price: '159900000', size: '2000000000' }, // $159.90, 2 SOL
							{ price: '159850000', size: '1500000000' }, // $159.85, 1.5 SOL
						],
						asks: [
							{ price: '160050000', size: '1500000000' }, // $160.05, 1.5 SOL
							{ price: '160100000', size: '1000000000' }, // $160.10, 1 SOL
							{ price: '160150000', size: '2000000000' }, // $160.15, 2 SOL
						],
					};
				}
				return null;
			});

			const result = await mapToMarketOrderParams(
				validParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(result.success).toBe(true);
			expect(result.data).toBeDefined();
			expect(result.data?.marketOrderParams).toBeDefined();
			expect(result.data?.estimatedPrices).toBeDefined();
			expect(result.error).toBeUndefined();

			// Verify price calculations
			const prices = result.data?.estimatedPrices;
			expect(prices?.entryPrice.gt(ZERO)).toBe(true);
			expect(prices?.bestPrice.gt(ZERO)).toBe(true);
			expect(prices?.worstPrice.gt(ZERO)).toBe(true);
			expect(prices?.oraclePrice.gt(ZERO)).toBe(true);

			// Verify market order params structure
			const marketOrderParams = result.data?.marketOrderParams;
			expect(marketOrderParams?.marketType).toBe(MarketType.PERP);
			expect(marketOrderParams?.direction).toBe(PositionDirection.LONG);
			expect(marketOrderParams?.baseAmount).toBeDefined();
			expect(marketOrderParams?.baseAmount.gt(ZERO)).toBe(true);
		});

		it('should handle quote-to-base conversion with realistic prices', async () => {
			const solPrice = 160; // $160 SOL price
			const quoteAmount = 1000; // $1,000 worth
			const quoteAmountInPrecision = new BN(quoteAmount).mul(QUOTE_PRECISION); // Convert to quote precision

			mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
				price: new BN(solPrice).mul(PRICE_PRECISION),
			});

			mockFetchFromRedis.mockImplementation(async (key: string) => {
				if (key.includes('dlob_orderbook')) {
					return {
						bids: [
							{ price: '160000000', size: '1000000000' }, // $160.00, 1 SOL
						],
						asks: [
							{ price: '160000000', size: '1000000000' }, // $160.00, 1 SOL
						],
					};
				}
				return null;
			});

			const quoteParams = {
				...validParams,
				amount: quoteAmountInPrecision.toString(),
				assetType: 'quote' as any,
			};

			const result = await mapToMarketOrderParams(
				quoteParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(result.success).toBe(true);

			// With $1,000 at $160 per SOL, we should get ~6.25 SOL
			const baseAmount = result.data?.marketOrderParams.baseAmount;
			expect(baseAmount).toBeDefined();
			expect(baseAmount.gt(ZERO)).toBe(true);

			// Calculate expected base amount: (amount * BASE_PRECISION) / entryPrice
			const expectedBaseAmount = quoteAmountInPrecision
				.mul(BASE_PRECISION)
				.div(result.data?.estimatedPrices.entryPrice);

			expect(baseAmount.toString()).toBe(expectedBaseAmount.toString());
		});

		it('should handle different market types correctly', async () => {
			const usdcPrice = 1; // $1 USDC price

			mockDriftClient.getOracleDataForSpotMarket.mockReturnValue({
				price: new BN(usdcPrice).mul(PRICE_PRECISION),
			});

			mockFetchFromRedis.mockImplementation(async (key: string) => {
				if (key.includes('dlob_orderbook')) {
					return {
						bids: [
							{ price: '1000000', size: '10000000000' }, // $1.00, 10,000 USDC
						],
						asks: [
							{ price: '1000000', size: '10000000000' }, // $1.00, 10,000 USDC
						],
					};
				}
				return null;
			});

			const spotParams = {
				...validParams,
				marketType: 'spot' as any,
			};

			const result = await mapToMarketOrderParams(
				spotParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(result.success).toBe(true);
			expect(result.data?.marketOrderParams.marketType).toBe(MarketType.SPOT);
			expect(result.data?.estimatedPrices.entryPrice.gt(ZERO)).toBe(true);
		});

		it('should handle different directions correctly', async () => {
			const solPrice = 160; // $160 SOL price

			mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
				price: new BN(solPrice).mul(PRICE_PRECISION),
			});

			mockFetchFromRedis.mockImplementation(async (key: string) => {
				if (key.includes('dlob_orderbook')) {
					return {
						bids: [
							{ price: '159950000', size: '1000000000' }, // $159.95, 1 SOL
						],
						asks: [
							{ price: '160050000', size: '1000000000' }, // $160.05, 1 SOL
						],
					};
				}
				return null;
			});

			const shortParams = {
				...validParams,
				direction: 'short' as any,
			};

			const result = await mapToMarketOrderParams(
				shortParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(result.success).toBe(true);
			expect(result.data?.marketOrderParams.direction).toBe(
				PositionDirection.SHORT
			);
			expect(result.data?.estimatedPrices.entryPrice.gt(ZERO)).toBe(true);
		});

		it('should handle various order sizes with L2 depth', async () => {
			const solPrice = 160; // $160 SOL price

			mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
				price: new BN(solPrice).mul(PRICE_PRECISION),
			});

			// Mock L2 with multiple levels of depth
			mockFetchFromRedis.mockImplementation(async (key: string) => {
				if (key.includes('dlob_orderbook')) {
					return {
						bids: [
							{ price: '159950000', size: '500000000' }, // $159.95, 0.5 SOL
							{ price: '159900000', size: '1000000000' }, // $159.90, 1 SOL
							{ price: '159850000', size: '2000000000' }, // $159.85, 2 SOL
						],
						asks: [
							{ price: '160050000', size: '500000000' }, // $160.05, 0.5 SOL
							{ price: '160100000', size: '1000000000' }, // $160.10, 1 SOL
							{ price: '160150000', size: '2000000000' }, // $160.15, 2 SOL
						],
					};
				}
				return null;
			});

			// Test small order
			const smallOrderParams = {
				...validParams,
				amount: new BN(10).mul(BASE_PRECISION).toString(), // 10 in BASE_PRECISION
			};

			const smallResult = await mapToMarketOrderParams(
				smallOrderParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(smallResult.success).toBe(true);
			expect(smallResult.data?.estimatedPrices.entryPrice.gt(ZERO)).toBe(true);

			// Test large order
			const largeOrderParams = {
				...validParams,
				amount: new BN(1000).mul(BASE_PRECISION).toString(), // 1000 in BASE_PRECISION
			};

			const largeResult = await mapToMarketOrderParams(
				largeOrderParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(largeResult.success).toBe(true);
			expect(largeResult.data?.estimatedPrices.entryPrice.gt(ZERO)).toBe(true);

			// Large orders should have higher price impact
			expect(
				largeResult.data?.estimatedPrices.priceImpact.gte(
					smallResult.data?.estimatedPrices.priceImpact
				)
			).toBe(true);
		});

		it('should handle zero oracle price scenario', async () => {
			mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
				price: ZERO,
			});

			mockFetchFromRedis.mockImplementation(async (key: string) => {
				if (key.includes('dlob_orderbook')) {
					return {
						bids: [
							{ price: '159950000', size: '1000000000' }, // $159.95, 1 SOL
						],
						asks: [
							{ price: '160050000', size: '1000000000' }, // $160.05, 1 SOL
						],
					};
				}
				return null;
			});

			const result = await mapToMarketOrderParams(
				validParams,
				mockDriftClient as any,
				mockFetchFromRedis as any,
				mockSelectMostRecentBySlot as any
			);

			expect(result.success).toBe(true);
			expect(result.data).toBeDefined();
			expect(result.data?.estimatedPrices.oraclePrice.eq(ZERO)).toBe(true);
			// Entry price should be calculated from L2 data even if oracle price is zero
			expect(result.data?.estimatedPrices.entryPrice).toBeDefined();
		});
	});

	describe('formatAuctionParamsForResponse', () => {
		it('should format BN values to strings', () => {
			const mockAuctionParams = {
				auctionStartPriceOffsetFrom: 'mark',
				auctionStartPriceOffset: new BN(0),
				auctionEndPriceOffsetFrom: 'best',
				auctionEndPriceOffset: new BN(100000), // 0.1 with PRICE_PRECISION (1e6)
				auctionDuration: 30,
				slippageTolerance: 0.05,
				additionalEndPriceBuffer: new BN(50000), // 0.05 with PRICE_PRECISION
				userOrderId: 123,
				isOracleOrder: false,
			};

			const result = formatAuctionParamsForResponse(mockAuctionParams);

			expect(result.auctionStartPriceOffsetFrom).toBe('mark');
			expect(result.auctionStartPriceOffset).toBe('0');
			expect(result.auctionEndPriceOffsetFrom).toBe('best');
			expect(result.auctionEndPriceOffset).toBe('100000');
			expect(result.auctionDuration).toBe(30);
			expect(result.slippageTolerance).toBe(0.05);
			expect(result.additionalEndPriceBuffer).toBe('50000');
			expect(result.userOrderId).toBe(123);
			expect(result.isOracleOrder).toBe(false);
		});

		it('should handle different BN values correctly', () => {
			const mockAuctionParams = {
				auctionStartPriceOffsetFrom: 'oracle',
				auctionStartPriceOffset: new BN(250000), // 0.25 with PRICE_PRECISION
				auctionEndPriceOffsetFrom: 'worst',
				auctionEndPriceOffset: new BN(1500000), // 1.5 with PRICE_PRECISION
				auctionDuration: 60,
				slippageTolerance: 0.1,
				additionalEndPriceBuffer: new BN(0), // 0 with PRICE_PRECISION
				userOrderId: 456,
				isOracleOrder: true,
			};

			const result = formatAuctionParamsForResponse(mockAuctionParams);

			expect(result.auctionStartPriceOffset).toBe('250000');
			expect(result.auctionEndPriceOffset).toBe('1500000');
			expect(result.additionalEndPriceBuffer).toBe('0');
			expect(result.isOracleOrder).toBe(true);
		});

		it('should handle undefined/null values gracefully', () => {
			const mockAuctionParams = {
				auctionStartPriceOffsetFrom: 'mark',
				auctionStartPriceOffset: new BN(0),
				auctionEndPriceOffsetFrom: 'mark',
				auctionEndPriceOffset: new BN(100000),
				auctionDuration: 30,
				slippageTolerance: undefined,
				additionalEndPriceBuffer: undefined,
				userOrderId: undefined,
				isOracleOrder: undefined,
			};

			const result = formatAuctionParamsForResponse(mockAuctionParams);

			expect(result.auctionStartPriceOffsetFrom).toBe('mark');
			expect(result.auctionStartPriceOffset).toBe('0');
			expect(result.auctionEndPriceOffsetFrom).toBe('mark');
			expect(result.auctionEndPriceOffset).toBe('100000');
			expect(result.auctionDuration).toBe(30);
			expect(result.slippageTolerance).toBeUndefined();
			expect(result.additionalEndPriceBuffer).toBeUndefined();
			expect(result.userOrderId).toBeUndefined();
			expect(result.isOracleOrder).toBeUndefined();
		});
	});

	describe('Business Logic Integration', () => {
		it('should demonstrate complete parameter flow for different market types', () => {
			const scenarios = [
				{
					name: 'Major PERP market with market-based params',
					input: {
						marketIndex: 0,
						marketType: 'perp' as any,
						direction: 'long' as any,
						amount: new BN(100).mul(BASE_PRECISION).toString(), // 100 in BASE_PRECISION
						assetType: 'base' as any,
						auctionStartPriceOffsetFrom: 'marketBased' as any,
						auctionStartPriceOffset: 'marketBased' as any,
					},
					expectedOffset: { from: 'mark', value: 0 },
				},
				{
					name: 'Minor PERP market with market-based params',
					input: {
						marketIndex: 5,
						marketType: 'perp' as any,
						direction: 'short' as any,
						amount: new BN(500).mul(QUOTE_PRECISION).toString(), // 500 in QUOTE_PRECISION
						assetType: 'quote' as any,
						auctionStartPriceOffsetFrom: 'marketBased' as any,
						auctionStartPriceOffset: 'marketBased' as any,
					},
					expectedOffset: { from: 'bestOffer', value: -0.1 },
				},
				{
					name: 'SPOT market with market-based params',
					input: {
						marketIndex: 2,
						marketType: 'spot' as any,
						direction: 'long' as any,
						amount: new BN(1000).mul(BASE_PRECISION).toString(), // 1000 in BASE_PRECISION
						assetType: 'base' as any,
						auctionStartPriceOffsetFrom: 'marketBased' as any,
						auctionStartPriceOffset: 'marketBased' as any,
					},
					expectedOffset: { from: 'bestOffer', value: -0.1 },
				},
			];

			scenarios.forEach((scenario) => {
				const result = createMarketBasedAuctionParams(scenario.input);

				expect(result.auctionStartPriceOffsetFrom).toBe(
					scenario.expectedOffset.from
				);
				expect(result.auctionStartPriceOffset).toBe(
					scenario.expectedOffset.value
				);

				// Verify all original parameters are preserved
				expect(result.marketIndex).toBe(scenario.input.marketIndex);
				expect(result.marketType).toBe(scenario.input.marketType);
				expect(result.direction).toBe(scenario.input.direction);
				expect(result.amount).toBe(scenario.input.amount);
				expect(result.assetType).toBe(scenario.input.assetType);
			});
		});

		it('should handle parameter precedence correctly', () => {
			// Test that explicit values always take precedence over market-based logic
			const input = {
				marketIndex: 0, // Major market - would normally get 'mark' and 0
				marketType: 'perp' as any,
				direction: 'long' as any,
				amount: new BN(100).mul(BASE_PRECISION).toString(), // 100 in BASE_PRECISION
				assetType: 'base' as any,
				auctionStartPriceOffsetFrom: 'oracle' as any, // explicit value
				auctionStartPriceOffset: 0.25, // explicit value
				auctionEndPriceOffset: 0.5, // explicit value
				auctionDuration: 45, // explicit value
			};

			const result = createMarketBasedAuctionParams(input);

			// Should use explicit values, not market-based defaults
			expect(result.auctionStartPriceOffsetFrom).toBe('oracle');
			expect(result.auctionStartPriceOffset).toBe(0.25);
			expect(result.auctionEndPriceOffset).toBe(0.5);
			expect(result.auctionDuration).toBe(45);
		});

		it('should demonstrate end-to-end parameter formatting', () => {
			// Create auction params with market-based logic
			const input = {
				marketIndex: 1,
				marketType: 'perp' as any,
				direction: 'long' as any,
				amount: new BN(100).mul(BASE_PRECISION).toString(), // 100 in BASE_PRECISION
				assetType: 'base' as any,
				auctionStartPriceOffsetFrom: 'marketBased' as any,
				auctionStartPriceOffset: 'marketBased' as any,
				auctionDuration: 30,
			};

			const auctionParams = createMarketBasedAuctionParams(input);

			// Simulate the structure that would come from deriveMarketOrderParams
			const mockDerivedParams = {
				auctionStartPriceOffsetFrom: auctionParams.auctionStartPriceOffsetFrom,
				auctionStartPriceOffset: new BN(0), // 0 for major market
				auctionEndPriceOffsetFrom: auctionParams.auctionEndPriceOffsetFrom,
				auctionEndPriceOffset: new BN(100000), // 0.1 with PRICE_PRECISION
				auctionDuration: auctionParams.auctionDuration,
				slippageTolerance: 0.05,
				additionalEndPriceBuffer: new BN(50000), // 0.05 with PRICE_PRECISION
				userOrderId: 123,
				isOracleOrder: true,
			};

			const formattedResponse =
				formatAuctionParamsForResponse(mockDerivedParams);

			// Verify the complete flow
			expect(formattedResponse.auctionStartPriceOffsetFrom).toBe('mark'); // Market-based for major market
			expect(formattedResponse.auctionStartPriceOffset).toBe('0'); // Market-based for major market
			expect(formattedResponse.auctionEndPriceOffset).toBe('100000'); // Formatted from BN
			expect(formattedResponse.auctionDuration).toBe(30); // Preserved from input
			expect(formattedResponse.slippageTolerance).toBe(0.05); // Preserved as number
			expect(formattedResponse.additionalEndPriceBuffer).toBe('50000'); // Formatted from BN
			expect(formattedResponse.userOrderId).toBe(123); // Preserved as number
			expect(formattedResponse.isOracleOrder).toBe(true); // Preserved as boolean
		});
	});
});

describe('calculateDynamicSlippage - crossed book handling', () => {
	const mockDriftClient = {
		getOracleDataForPerpMarket: jest.fn(),
		getOracleDataForSpotMarket: jest.fn(),
	} as any;

	beforeEach(() => {
		jest.clearAllMocks();
	});

	it('caps spread contribution when crossed (default cap mode)', () => {
		// Set env to deterministic values
		process.env.DYNAMIC_BASE_SLIPPAGE_MAJOR = '0';
		process.env.DYNAMIC_SLIPPAGE_MULTIPLIER_MAJOR = '1';
		process.env.DYNAMIC_SLIPPAGE_MIN = '0';
		process.env.DYNAMIC_SLIPPAGE_MAX = '100';
		delete process.env.DYNAMIC_CROSS_SPREAD_MODE; // default 'cap'
		process.env.DYNAMIC_CROSS_SPREAD_CAP = '0.1'; // 0.1%

		// Oracle 100
		mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
			price: new BN(100).mul(PRICE_PRECISION),
		});

		const l2Crossed = {
			bids: [{ price: new BN(101).mul(PRICE_PRECISION), size: new BN(1) }],
			asks: [{ price: new BN(99).mul(PRICE_PRECISION), size: new BN(1) }],
		} as any;

		const startPrice = new BN(100).mul(PRICE_PRECISION);
		const worstPrice = new BN(100).mul(PRICE_PRECISION);

		const slip = calculateDynamicSlippage(
			'long',
			0, // major perp
			'perp',
			mockDriftClient,
			l2Crossed,
			startPrice,
			worstPrice
		);

		// Should be capped at 0.1% given our env
		expect(slip).toBeLessThanOrEqual(0.1);
		expect(slip).toBeGreaterThanOrEqual(0);
	});

	it('normal (non-crossed) book produces spread-based slippage > crossed-capped', () => {
		process.env.DYNAMIC_BASE_SLIPPAGE_MAJOR = '0';
		process.env.DYNAMIC_SLIPPAGE_MULTIPLIER_MAJOR = '1';
		process.env.DYNAMIC_SLIPPAGE_MIN = '0';
		process.env.DYNAMIC_SLIPPAGE_MAX = '100';
		delete process.env.DYNAMIC_CROSS_SPREAD_MODE; // default cap
		process.env.DYNAMIC_CROSS_SPREAD_CAP = '0.1';

		mockDriftClient.getOracleDataForPerpMarket.mockReturnValue({
			price: new BN(100).mul(PRICE_PRECISION),
		});

		const l2Normal = {
			bids: [{ price: new BN(99).mul(PRICE_PRECISION), size: new BN(1) }],
			asks: [{ price: new BN(101).mul(PRICE_PRECISION), size: new BN(1) }],
		} as any;

		const startPrice = new BN(100).mul(PRICE_PRECISION);
		const worstPrice = new BN(100).mul(PRICE_PRECISION);

		const slipNormal = calculateDynamicSlippage(
			'long',
			0,
			'perp',
			mockDriftClient,
			l2Normal,
			startPrice,
			worstPrice
		);

		const l2Crossed = {
			bids: [{ price: new BN(101).mul(PRICE_PRECISION), size: new BN(1) }],
			asks: [{ price: new BN(99).mul(PRICE_PRECISION), size: new BN(1) }],
		} as any;

		const slipCrossed = calculateDynamicSlippage(
			'long',
			0,
			'perp',
			mockDriftClient,
			l2Crossed,
			startPrice,
			worstPrice
		);

		// Non-crossed spread should be >= crossed (which is capped)
		expect(slipNormal).toBeGreaterThanOrEqual(slipCrossed);
	});
});

describe('calculateDynamicSlippage - 1bp best bid/ask adjustment', () => {
	const mockDriftClient = {
		getMMOracleDataForPerpMarket: jest.fn(),
		getOracleDataForSpotMarket: jest.fn(),
	} as any;

	beforeEach(() => {
		jest.clearAllMocks();
		// Set deterministic env values
		process.env.DYNAMIC_BASE_SLIPPAGE_MAJOR = '0';
		process.env.DYNAMIC_SLIPPAGE_MULTIPLIER_MAJOR = '1';
		process.env.DYNAMIC_SLIPPAGE_MIN = '0.01'; // 0.01% minimum
		process.env.DYNAMIC_SLIPPAGE_MAX = '100';
	});

	it('should adjust slippage for LONG when impliedEndPrice < bestAskPrice', () => {
		// Set very tight env to minimize base slippage
		process.env.DYNAMIC_BASE_SLIPPAGE_MAJOR = '0';
		process.env.DYNAMIC_SLIPPAGE_MIN = '0';
		
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(100).mul(PRICE_PRECISION); // $100 (tight spread)
		const bestAskPrice = new BN(101).mul(PRICE_PRECISION); // $101
		const startPrice = new BN(99).mul(PRICE_PRECISION); // $99 (start below ask)

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		// With start price at 99 and very small worst price, initial slippage will be tiny
		const worstPrice = new BN(99100000); // $99.10 in PRICE_PRECISION

		const slippage = calculateDynamicSlippage(
			'long',
			0, // major perp
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Calculate what the implied end price would be
		const impliedEndPrice = startPrice.toNumber() * (1 + slippage / 100);

		// Verify that impliedEndPrice is at least 1bp greater than bestAskPrice
		const targetPrice = bestAskPrice.toNumber() * 1.0001; // 1bp above bestAsk
		expect(impliedEndPrice).toBeGreaterThanOrEqual(targetPrice);

		// With startPrice at 99 and bestAsk at 101, we need at least 2.02% slippage
		// to reach 101 * 1.0001 = 101.0101
		expect(slippage).toBeGreaterThan(2.0);
		expect(slippage).toBeLessThan(3.0);
	});

	it('should adjust slippage for SHORT when impliedEndPrice > bestBidPrice', () => {
		// Set very tight env to minimize base slippage
		process.env.DYNAMIC_BASE_SLIPPAGE_MAJOR = '0';
		process.env.DYNAMIC_SLIPPAGE_MIN = '0';
		
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(99).mul(PRICE_PRECISION); // $99
		const bestAskPrice = new BN(100).mul(PRICE_PRECISION); // $100 (tight spread)
		const startPrice = new BN(101).mul(PRICE_PRECISION); // $101 (start above bid)

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		// With start price at 101 and very small worst price, initial slippage will be tiny
		const worstPrice = new BN(100900000); // $100.90 in PRICE_PRECISION

		const slippage = calculateDynamicSlippage(
			'short',
			0, // major perp
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Calculate what the implied end price would be
		const impliedEndPrice = startPrice.toNumber() * (1 - slippage / 100);

		// Verify that impliedEndPrice is at least 1bp less than bestBidPrice
		const targetPrice = bestBidPrice.toNumber() * 0.9999; // 1bp below bestBid
		expect(impliedEndPrice).toBeLessThanOrEqual(targetPrice);

		// With startPrice at 101 and bestBid at 99, we need at least 1.99% slippage
		// to reach 99 * 0.9999 = 98.9901
		expect(slippage).toBeGreaterThan(1.98);
		expect(slippage).toBeLessThan(3.0);
	});

	it('should NOT adjust slippage for LONG when impliedEndPrice already > bestAskPrice', () => {
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(99).mul(PRICE_PRECISION); // $99
		const bestAskPrice = new BN(101).mul(PRICE_PRECISION); // $101
		const startPrice = new BN(100).mul(PRICE_PRECISION); // $100

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		// With large worst price, impliedEndPrice will already be > bestAskPrice
		const worstPrice = new BN(105).mul(PRICE_PRECISION); // $105 - large slippage

		const slippage = calculateDynamicSlippage(
			'long',
			0, // major perp
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Calculate what the implied end price would be
		const impliedEndPrice = startPrice.toNumber() * (1 + slippage / 100);

		// Should already be well above bestAskPrice
		expect(impliedEndPrice).toBeGreaterThan(bestAskPrice.toNumber());

		// Slippage should be driven by the size-adjusted calculation (halfway to worst)
		// Expected: ((100 - 105) / 100 / 2) * 100 = 2.5%
		expect(slippage).toBeGreaterThan(2.0);
		expect(slippage).toBeLessThan(3.0);
	});

	it('should NOT adjust slippage for SHORT when impliedEndPrice already < bestBidPrice', () => {
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(99).mul(PRICE_PRECISION); // $99
		const bestAskPrice = new BN(101).mul(PRICE_PRECISION); // $101
		const startPrice = new BN(100).mul(PRICE_PRECISION); // $100

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		// With large worst price, impliedEndPrice will already be < bestBidPrice
		const worstPrice = new BN(95).mul(PRICE_PRECISION); // $95 - large slippage

		const slippage = calculateDynamicSlippage(
			'short',
			0, // major perp
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Calculate what the implied end price would be
		const impliedEndPrice = startPrice.toNumber() * (1 - slippage / 100);

		// Should already be well below bestBidPrice
		expect(impliedEndPrice).toBeLessThan(bestBidPrice.toNumber());

		// Slippage should be driven by the size-adjusted calculation
		expect(slippage).toBeGreaterThan(2.0);
		expect(slippage).toBeLessThan(3.0);
	});

	it('should handle tight spread with precise 1bp adjustment for LONG', () => {
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(99950000); // $99.95 in PRICE_PRECISION
		const bestAskPrice = new BN(100050000); // $100.05 in PRICE_PRECISION (5bp spread)
		const startPrice = new BN(100).mul(PRICE_PRECISION); // $100

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		const worstPrice = new BN(100010000); // $100.01 in PRICE_PRECISION

		const slippage = calculateDynamicSlippage(
			'long',
			0,
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Calculate implied end price
		const impliedEndPrice = startPrice.toNumber() * (1 + slippage / 100);
		const targetPrice = bestAskPrice.toNumber() * 1.0001;

		// Should be adjusted to go 1bp past bestAsk
		expect(impliedEndPrice).toBeGreaterThanOrEqual(targetPrice);

		// The slippage will be influenced by spread calculation (5bp spread * 0.9 = 0.45%)
		// plus size adjustment, so it will be higher than just the 1bp adjustment
		expect(slippage).toBeGreaterThan(0.05);
		expect(slippage).toBeLessThan(1.5);
	});

	it('should handle tight spread with precise 1bp adjustment for SHORT', () => {
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(99950000); // $99.95 in PRICE_PRECISION
		const bestAskPrice = new BN(100050000); // $100.05 in PRICE_PRECISION (5bp spread)
		const startPrice = new BN(100).mul(PRICE_PRECISION); // $100

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		const worstPrice = new BN(99990000); // $99.99 in PRICE_PRECISION

		const slippage = calculateDynamicSlippage(
			'short',
			0,
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Calculate implied end price
		const impliedEndPrice = startPrice.toNumber() * (1 - slippage / 100);
		const targetPrice = bestBidPrice.toNumber() * 0.9999;

		// Should be adjusted to go 1bp past bestBid
		expect(impliedEndPrice).toBeLessThanOrEqual(targetPrice);

		// The slippage will be influenced by spread calculation (5bp spread * 0.9 = 0.45%)
		// plus the 1bp adjustment needed, so it will be higher than just 0.06%
		expect(slippage).toBeGreaterThan(0.05);
		expect(slippage).toBeLessThan(1.5);
	});

	it('should handle crossed orderbook for LONG', () => {
		const oraclePrice = new BN(100).mul(PRICE_PRECISION); // $100
		const bestBidPrice = new BN(101).mul(PRICE_PRECISION); // $101 (crossed)
		const bestAskPrice = new BN(99).mul(PRICE_PRECISION); // $99 (crossed)
		const startPrice = new BN(100).mul(PRICE_PRECISION); // $100

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		const worstPrice = new BN(100010000); // $100.01 in PRICE_PRECISION

		const slippage = calculateDynamicSlippage(
			'long',
			0,
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Even with crossed book, should still calculate correctly
		// bestAskPrice is 99, so targetPrice = 99 * 1.0001 = 99.0099
		// impliedEndPrice should be >= 99.0099
		const impliedEndPrice = startPrice.toNumber() * (1 + slippage / 100);
		
		// Since bestAsk (99) < startPrice (100), the adjustment shouldn't trigger
		// because impliedEndPrice will already be > bestAsk
		expect(impliedEndPrice).toBeGreaterThan(bestAskPrice.toNumber());
	});

	it('should respect minimum slippage even with adjustment', () => {
		process.env.DYNAMIC_SLIPPAGE_MIN = '2.0'; // 2% minimum

		const oraclePrice = new BN(100).mul(PRICE_PRECISION);
		const bestBidPrice = new BN(99).mul(PRICE_PRECISION);
		const bestAskPrice = new BN(101).mul(PRICE_PRECISION);
		const startPrice = new BN(100).mul(PRICE_PRECISION);

		mockDriftClient.getMMOracleDataForPerpMarket.mockReturnValue({
			price: oraclePrice,
		});

		const l2 = {
			bids: [{ price: bestBidPrice, size: new BN(1) }],
			asks: [{ price: bestAskPrice, size: new BN(1) }],
		} as any;

		const worstPrice = new BN(100010000); // $100.01 in PRICE_PRECISION

		const slippage = calculateDynamicSlippage(
			'long',
			0,
			'perp',
			mockDriftClient,
			l2,
			startPrice,
			worstPrice
		);

		// Should respect the 2% minimum
		expect(slippage).toBeGreaterThanOrEqual(2.0);
	});
});
