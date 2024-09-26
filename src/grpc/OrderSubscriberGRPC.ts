import { BorshAccountsCoder } from '@coral-xyz/anchor';
import { Commitment, PublicKey, RpcResponseAndContext } from '@solana/web3.js';
import { Buffer } from 'buffer';
import Client, {
	CommitmentLevel,
	SubscribeRequest,
	SubscribeUpdate,
} from '@triton-one/yellowstone-grpc';
import {
	BN,
	DLOB,
	DriftClient,
	UserAccount,
	getUserFilter,
	getUserWithOrderFilter,
} from '@drift-labs/sdk';
import { ClientDuplexStream } from '@grpc/grpc-js';

type grpcDlobConfig = {
	endpoint: string;
	token: string;
};

export class GeyserOrderSubscriber {
	usersAccounts = new Map<string, { slot: number; userAccount: UserAccount }>();
	commitment: Commitment;
	driftClient: DriftClient;
	config: grpcDlobConfig;
	stream: ClientDuplexStream<SubscribeRequest, SubscribeUpdate>;

	fetchPromise?: Promise<void>;
	fetchPromiseResolver: () => void;
	mostRecentSlot: number;

	constructor(driftClient: DriftClient, config: grpcDlobConfig) {
		this.driftClient = driftClient;
		this.config = config;
	}

	public async subscribe(): Promise<void> {
		const client = new Client(this.config.endpoint, this.config.token);
		this.stream = await client.subscribe();
		const request: SubscribeRequest = {
			slots: {},
			accounts: {
				drift: {
					owner: [this.driftClient.program.programId.toBase58()],
					filters: [
						{
							memcmp: {
								offset: '0',
								bytes: BorshAccountsCoder.accountDiscriminator('User'),
							},
						},
						{
							memcmp: {
								offset: '4350',
								bytes: Uint8Array.from([1]),
							},
						},
					],
					account: [],
				},
			},
			transactions: {},
			blocks: {},
			blocksMeta: {},
			accountsDataSlice: [],
			commitment: CommitmentLevel.PROCESSED,
			entry: {},
		};

		this.stream.on('data', (chunk: any) => {
			if (!chunk.account) {
				return;
			}
			const slot = Number(chunk.account.slot);
			this.tryUpdateUserAccount(
				new PublicKey(chunk.account.account.pubkey).toBase58(),
				'grpc',
				chunk.account.account.data,
				slot
			);
		});

		return new Promise<void>((resolve, reject) => {
			this.stream.write(request, (err) => {
				if (err === null || err === undefined) {
					resolve();
				} else {
					reject(err);
				}
			});
		}).catch((reason) => {
			console.error(reason);
			throw reason;
		});
	}

	async fetch(): Promise<void> {
		if (this.fetchPromise) {
			return this.fetchPromise;
		}

		this.fetchPromise = new Promise((resolver) => {
			this.fetchPromiseResolver = resolver;
		});

		try {
			const rpcRequestArgs = [
				this.driftClient.program.programId.toBase58(),
				{
					commitment: this.commitment,
					filters: [getUserFilter(), getUserWithOrderFilter()],
					encoding: 'base64',
					withContext: true,
				},
			];

			const rpcJSONResponse: any =
				// @ts-ignore
				await this.driftClient.connection._rpcRequest(
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

			const programAccountSet = new Set<string>();
			for (const programAccount of rpcResponseAndContext.value) {
				const key = programAccount.pubkey.toString();
				programAccountSet.add(key);
				this.tryUpdateUserAccount(
					key,
					'raw',
					programAccount.account.data,
					slot
				);
				// give event loop a chance to breathe
				await new Promise((resolve) => setTimeout(resolve, 0));
			}

			for (const key of this.usersAccounts.keys()) {
				if (!programAccountSet.has(key)) {
					this.usersAccounts.delete(key);
				}
				// give event loop a chance to breathe
				await new Promise((resolve) => setTimeout(resolve, 0));
			}
		} catch (e) {
			console.error(e);
		} finally {
			this.fetchPromiseResolver();
			this.fetchPromise = undefined;
		}
	}

	tryUpdateUserAccount(
		key: string,
		dataType: 'raw' | 'grpc',
		data: string[] | Buffer | UserAccount,
		slot: number
	): void {
		if (!this.mostRecentSlot || slot > this.mostRecentSlot) {
			this.mostRecentSlot = slot;
		}

		const slotAndUserAccount = this.usersAccounts.get(key);
		if (!slotAndUserAccount || slotAndUserAccount.slot <= slot) {
			// Polling leads to a lot of redundant decoding, so we only decode if data is from a fresh slot
			let buffer: Buffer;
			if (dataType === 'raw') {
				buffer = Buffer.from(data[0], data[1]);
			} else {
				buffer = data as Buffer;
			}

			const newLastActiveSlot = new BN(
				buffer.subarray(4328, 4328 + 8),
				undefined,
				'le'
			);
			if (
				slotAndUserAccount &&
				slotAndUserAccount.userAccount.lastActiveSlot.gt(newLastActiveSlot)
			) {
				return;
			}

			const userAccount =
				this.driftClient.program.account.user.coder.accounts.decodeUnchecked(
					'User',
					buffer
				) as UserAccount;

			if (userAccount.hasOpenOrder) {
				this.usersAccounts.set(key, { slot, userAccount });
			} else {
				this.usersAccounts.delete(key);
			}
		}
	}

	public async getDLOB(slot: number): Promise<DLOB> {
		const dlob = new DLOB();
		for (const [key, { userAccount }] of this.usersAccounts.entries()) {
			const userAccountPubkey = new PublicKey(key);
			for (const order of userAccount.orders) {
				dlob.insertOrder(order, userAccountPubkey.toBase58(), slot);
			}
		}
		return dlob;
	}

	public getSlot(): number {
		return this.mostRecentSlot ?? 0;
	}

	public async unsubscribe(): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			this.stream.write(
				{
					slots: {},
					accounts: {},
					transactions: {},
					blocks: {},
					blocksMeta: {},
					accountsDataSlice: [],w
					entry: {},
				},
				(err) => {
					if (err === null || err === undefined) {
						resolve();
					} else {
						reject(err);
					}
				}
			);
		}).catch((reason) => {
			console.error(reason);
			throw reason;
		});
	}
}
