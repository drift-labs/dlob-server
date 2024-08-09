import { RedisClient, RedisClientPrefix, sleep } from '@drift/common';
import {
	BigNum,
	DriftClient,
	OneShotUserAccountSubscriber,
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
import { logger } from '../utils/logger';

const dotenv = require('dotenv');
dotenv.config();

logger.info('Starting script to publish unsettled pnl users');

type UserPnlMap = {
	[perpMarketIndex: number]: {
		gain: { userPubKey: string; pnl: number }[];
		loss: { userPubKey: string; pnl: number }[];
	};
};

type AllPnlUsers = {
	[perpMarketIndex: number]: {
		users: { userPubKey: string; pnl: number }[];
	};
};

const startTime = Date.now();
const endpoint = process.env.ENDPOINT as string;
const wsEndpoint = process.env.WS_ENDPOINT as string;
const driftEnv = process.env.ENV as string;
const chunkSize = Number(process.env.CHUNK_SIZE) || 100;

const connection = new Connection(endpoint, {
	commitment: 'confirmed',
	wsEndpoint: wsEndpoint,
});

const driftClient = new DriftClient({
	connection,
	wallet: new Wallet(new Keypair()),
});

const runningLocal = (process.env.RUNNING_LOCAL as string) === 'true';

const userMapRedisClient = runningLocal
	? new RedisClient({
			host: 'localhost',
			port: '6379',
			cluster: false,
			opts: {
				tls: null,
			},
	  })
	: new RedisClient({
			prefix: RedisClientPrefix.USER_MAP,
	  });

const dlobRedisClient = runningLocal
	? new RedisClient({
			host: 'localhost',
			port: '6378',
			cluster: false,
			opts: {
				tls: null,
			},
	  })
	: new RedisClient({
			prefix: RedisClientPrefix.DLOB,
	  });

const main = async () => {
	// get the users from usermap redis client
	await driftClient.subscribe();

	const userStrings = await userMapRedisClient.lRange('user_pubkeys', 0, -1);

	const totalCount = userStrings.length;
	let finishedCount = 0;
	let allNonZeroPnls = {};

	while (finishedCount < totalCount) {
		const redisUsers = await Promise.all(
			userStrings
				.slice(finishedCount, finishedCount + chunkSize)
				.map((userStr) => getUserFromRedis(userStr))
		);

		// add all the nonzero pnl users from each market in chunks
		allNonZeroPnls = buildUserMarketLists(redisUsers, allNonZeroPnls);

		finishedCount += chunkSize;

		console.log(`Wrote ${finishedCount} users, sleeping for 10s`);
		await sleep(10000);
	}

	const userPnlMap = createMarketPnlLeaderboards(allNonZeroPnls);

	const success = await writeToDlobRedis(userPnlMap);

	logger.info(
		`Unsettled PnL publisher ${
			success ? 'successfully completed' : 'failed'
		} in ${Date.now() - startTime} ms`
	);

	process.exit();
};

const writeToDlobRedis = async (userPnlMap: UserPnlMap): Promise<boolean> => {
	try {
		await Promise.all(
			Object.keys(userPnlMap).map(async (perpMarketIndex) => {
				const gainersRedisKey = `perp_market_${perpMarketIndex}_gainers`;
				const losersRedisKey = `perp_market_${perpMarketIndex}_losers`;

				// write the new lists
				await dlobRedisClient.setRaw(
					gainersRedisKey,
					JSON.stringify(userPnlMap[perpMarketIndex].gain)
				);
				await dlobRedisClient.setRaw(
					losersRedisKey,
					JSON.stringify(userPnlMap[perpMarketIndex].loss)
				);

				return;
			})
		);

		return true;
	} catch (e) {
		console.log('Error writing to dlob redis client: ', e);
		return false;
	}
};

const createMarketPnlLeaderboards = (allPnlUsers: AllPnlUsers): UserPnlMap => {
	let pnlMap = {};

	Object.keys(allPnlUsers).forEach((perpMarketIndex) => {
		const sortedPnls = allPnlUsers[perpMarketIndex].sort(
			(a, b) => b.pnl - a.pnl
		);
		// store the top 20 winners and losers in each perp market
		pnlMap[perpMarketIndex] = {
			gain: sortedPnls.slice(0, 20),
			loss: sortedPnls.slice(-20, -1),
		};
	});

	return pnlMap;
};

const buildUserMarketLists = (
	redisUsers: { user: User; bufferString: string }[],
	allPnlUsers: AllPnlUsers
): AllPnlUsers => {
	let newPnlUsers = allPnlUsers;

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

			const allNonZeroPnlsInMarket = redisUsers.flatMap((redisUser) => {
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
					logger.error(
						`Error reading pnl for user ${redisUser?.user?.userAccountPublicKey?.toString()}: `,
						e
					);
					return [];
				}
			});

			if (!allPnlUsers[perpMarket.marketIndex]) {
				newPnlUsers[perpMarket.marketIndex] = { users: allNonZeroPnlsInMarket };
			} else {
				newPnlUsers[perpMarket.marketIndex] = {
					users: allPnlUsers[perpMarket.marketIndex].users.concat(
						allNonZeroPnlsInMarket
					),
				};
			}
		} catch (e) {
			logger.error(`Could not fetch PnLs for ${perpMarket.symbol}: `, e);
		}
	}

	return newPnlUsers;
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
		logger.error(
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
