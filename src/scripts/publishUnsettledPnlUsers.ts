import { RedisClient, RedisClientPrefix } from '@drift/common';
import {
	DriftClient,
	OneShotUserAccountSubscriber,
	PublicKey,
	User,
	Wallet,
	decodeUser,
} from '@drift-labs/sdk';
import { Connection, Keypair } from '@solana/web3.js';

const dotenv = require('dotenv');
dotenv.config();

console.log('Starting script to publish unsettled pnl users');

const endpoint = process.env.ENDPOINT as string;
const wsEndpoint = process.env.WS_ENDPOINT as string;

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
	const userStrings = await userMapRedisClient.lRange('user_pubkeys', 0, 50);
	const usersAndBuffStrings = await Promise.all(
		userStrings.map((userStr) => {
			return getUserFromRedis(userStr);
		})
	);
	console.log(
		'user1 authority::: ',
		usersAndBuffStrings[0]?.user?.getUserAccount()?.authority?.toString()
	);
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
