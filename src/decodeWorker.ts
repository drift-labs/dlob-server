import { fork } from 'child_process';
import { BroadcastChannel, parentPort } from 'worker_threads';
import { DriftClient, UserAccount, Wallet } from '@drift-labs/sdk';
import { Connection, Keypair } from '@solana/web3.js';

const child = fork('./lib/gpaFetchProcess.js');
const bc = new BroadcastChannel('fetch-updates');

let driftClient: DriftClient;

parentPort.on(
	'message',
	async (message: {
		endpoint: string;
		wsEndpoint: string;
		source: 'orderSubscriber' | 'userMap';
		interval: number;
	}) => {
		driftClient = new DriftClient({
			connection: new Connection(message.endpoint, {
				commitment: 'processed',
				wsEndpoint: message.wsEndpoint,
			}),
			wallet: new Wallet(new Keypair()),
		});
		child.send(message);
	}
);

child.on('message', async (message) => {
	const userAccounts = [];
	if (message && message['userAccounts']) {
		for (const userAccountData of message['userAccounts']) {
			const buffer = Buffer.from(userAccountData['userAccount']['data']);
			const decodedAccount =
				driftClient.program.account.user.coder.accounts.decodeUnchecked(
					'User',
					buffer
				) as UserAccount;
			userAccounts.push(decodedAccount);
		}
	}
	bc.postMessage({ userAccounts });
});
