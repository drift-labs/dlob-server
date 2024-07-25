import { RedisClient, RedisClientPrefix } from '@drift/common';
import {
	BASE_PRECISION_EXP,
	BigNum,
	DriftClient,
	OneShotUserAccountSubscriber,
	PRICE_PRECISION_EXP,
	PerpMarkets,
	PublicKey,
	QUOTE_PRECISION_EXP,
	QUOTE_SPOT_MARKET_INDEX,
	User,
	Wallet,
	ZERO,
	calculateClaimablePnl,
	calculatePositionPNL,
	decodeUser,
} from '@drift-labs/sdk';
import { Connection, Keypair } from '@solana/web3.js';

const dotenv = require('dotenv');
dotenv.config();

console.log('Starting script to publish unsettled pnl users');

const endpoint = process.env.ENDPOINT as string;
const wsEndpoint = process.env.WS_ENDPOINT as string;
const driftEnv = process.env.ENV as string;

const connection = new Connection(endpoint, {
	commitment: 'confirmed',
	wsEndpoint: wsEndpoint,
});

const driftClient = new DriftClient({
	connection,
	wallet: new Wallet(new Keypair()),
});

const userMapRedisClient = new RedisClient({
	host: 'localhost',
	port: '6379',
	cluster: false,
	opts: {
		tls: null,
	},
	//prefix: RedisClientPrefix.USER_MAP,
});

// const dlobRedisClient = new RedisClient({
// 	host: 'localhost',
// 	port: '6378',
// 	prefix: RedisClientPrefix.DLOB,
// });

const main = async () => {
	// get the users from usermap redis client
	await driftClient.subscribe();

	const userStrings = await userMapRedisClient.lRange('user_pubkeys', 0, -1);
	const redisUsers = await Promise.all(
		userStrings.map((userStr) => getUserFromRedis(userStr))
	);

	// construct an object with the top 20/bottom 20 unsettled in each perp market
	const _userPnlMap = createMarketSpecificPnlLeaderboards(redisUsers);

	// publish the user keys with pnl to the dlob redis client
};

const createMarketSpecificPnlLeaderboards = (
	redisUsers: { user: User; bufferString: string }[]
): {
	[perpMarketIndex: number]: {
		gain: { userPubKey: string; pnl: number }[];
		loss: { userPubKey: string; pnl: number }[];
	};
} => {
	const pnlMap = {};

	const usdcSpotMarket = driftClient.getSpotMarketAccount(
		QUOTE_SPOT_MARKET_INDEX
	);

	for (const perpMarket of PerpMarkets[driftEnv]) {
		try {
			const perpMarketAccount = driftClient.getPerpMarketAccount(
				perpMarket.marketIndex
			);

			const oraclePriceData = driftClient.getOracleDataForPerpMarket(
				perpMarket.marketIndex
			);

			const allNonZeroPnls = redisUsers.flatMap((redisUser) => {
				try {
					const perpPosition = redisUser.user.getPerpPosition(
						perpMarket.marketIndex
					);

					if (
						!perpPosition ||
						(perpPosition.baseAssetAmount.eq(ZERO) &&
							perpPosition.quoteAssetAmount.eq(ZERO) &&
							perpPosition.lpShares.eq(ZERO))
					)
						return [];

					const perpPositionWithLpSettle =
						redisUser.user.getPerpPositionWithLPSettle(
							perpPosition.marketIndex,
							perpPosition,
							false
						)[0];

					let marketPnl = calculatePositionPNL(
						perpMarketAccount,
						perpPositionWithLpSettle,
						true,
						oraclePriceData
					);

					if (marketPnl.gt(ZERO)) {
						marketPnl = calculateClaimablePnl(
							perpMarketAccount,
							usdcSpotMarket,
							perpPositionWithLpSettle,
							oraclePriceData
						);
					}

					return marketPnl.eq(ZERO)
						? []
						: {
								userPubKey: redisUser.user.userAccountPublicKey.toString(),
								pnl: BigNum.from(marketPnl, QUOTE_PRECISION_EXP).toNum(),
						  };
				} catch (e) {
					console.log(
						`Error reading pnl for user ${redisUser?.user?.userAccountPublicKey?.toString()}: `,
						e
					);
					return [];
				}
			});

			const sortedPnls = allNonZeroPnls.sort((a, b) => b.pnl - a.pnl);

			// store the top 20 winners and losers in each perp market
			pnlMap[perpMarket.marketIndex] = {
				gain: sortedPnls.slice(0, 20),
				loss: sortedPnls.slice(-20, -1),
			};
		} catch (e) {
			console.log(`Could not fetch PnLs for ${perpMarket.symbol}: `, e);
		}
	}

	return pnlMap;
};

const getUserFromRedis = async (userAccountStr: string) => {
	const data = await userMapRedisClient.getRaw(userAccountStr);
	try {
		const bufferString = data.split('::')[1];
		const user = await createUserAccountFromBuffer(
			driftClient,
			userAccountStr,
			bufferString
		);
		return { user, bufferString };
	} catch (e) {
		console.log(
			`Error creating user account from buffer for user ${userAccountStr}`,
			e.message
		);
	}
};

const createUserAccountFromBuffer = async (
	driftClient: DriftClient,
	userAccountKey: string,
	bufferString: string
): Promise<User> => {
	const publicKey = new PublicKey(userAccountKey);
	const buffer = Buffer.from(bufferString, 'base64');
	const userAccount = decodeUser(buffer);
	const user = new User({
		driftClient: driftClient,
		userAccountPublicKey: publicKey,
		accountSubscription: {
			type: 'custom',
			userAccountSubscriber: new OneShotUserAccountSubscriber(
				driftClient.program,
				publicKey,
				userAccount
			),
		},
	});
	await user.subscribe(userAccount);
	return user;
};

main();
