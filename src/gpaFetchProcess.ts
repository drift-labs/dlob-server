import {
	DriftClient,
	PublicKey,
	Wallet,
	getUserFilter,
	getUserWithOrderFilter,
} from '@drift-labs/sdk';
import { Connection, Keypair, RpcResponseAndContext } from '@solana/web3.js';
import { Buffer } from 'buffer';

process.on(
	'message',
	async (message: {
		endpoint: string;
		wsEndpoint: string;
		source: 'orderSubscriber' | 'userMap';
		interval: number;
	}) => {
		const driftClient = new DriftClient({
			connection: new Connection(message.endpoint, {
				commitment: 'processed',
				wsEndpoint: message.wsEndpoint,
			}),
			wallet: new Wallet(new Keypair()),
		});

		if (message.source === 'orderSubscriber') {
			const fetch = async (): Promise<
				{
					slot: number;
					userAccountBuffer: Buffer;
				}[]
			> => {
				try {
					const rpcRequestArgs = [
						driftClient.program.programId.toBase58(),
						{
							commitment: 'processed',
							filters: [getUserFilter(), getUserWithOrderFilter()],
							encoding: 'base64',
							withContext: true,
						},
					];

					const rpcJSONResponse: any =
						// @ts-ignore
						await driftClient.connection._rpcRequest(
							'getProgramAccounts',
							rpcRequestArgs
						);

					const rpcResponseAndContext: RpcResponseAndContext<
						Array<{
							pubkey: PublicKey;
							account: {
								data: [string, string];
							};
						}>
					> = rpcJSONResponse.result;

					const slot: number = rpcResponseAndContext.context.slot;

					const programAccounts = [];
					for (const programAccount of rpcResponseAndContext.value) {
						programAccounts.push({
							slot,
							// @ts-ignore
							userAccount: Buffer.from(
								programAccount.account.data[0],
								programAccount.account.data[1]
							),
						});
					}
					return programAccounts;
				} catch (e) {
					console.error(e);
				}
			};

			const refresh = async () => {
				const userAccounts = await fetch();
				process.send({ userAccounts });
				// eslint-disable-next-line @typescript-eslint/no-unused-vars
				setTimeout(() => refresh(), message.interval);
			};
			refresh();
		}
	}
);
