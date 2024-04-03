import { program } from 'commander';

import { Connection, Commitment, PublicKey, Keypair } from '@solana/web3.js';

import {
	DriftClient,
	initialize,
	DriftEnv,
	SlotSubscriber,
	Wallet,
	EventSubscriber,
	OrderAction,
	convertToNumber,
	BASE_PRECISION,
	QUOTE_PRECISION,
	PRICE_PRECISION,
	getVariant,
	ZERO,
	BN,
	OrderActionRecord,
	Event,
} from '@drift-labs/sdk';

import { logger, setLogLevel } from '../utils/logger';
import { sleep } from '../utils/utils';
import { RedisClient } from '../utils/redisClient';
import { fromEvent, filter, map } from 'rxjs';

require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
let driftClient: DriftClient;

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const endpoint = process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

const main = async () => {
	const wallet = new Wallet(new Keypair());
	const clearingHousePublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);

	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	driftClient = new DriftClient({
		connection,
		wallet,
		programID: clearingHousePublicKey,
		accountSubscription: {
			type: 'websocket',
			commitment: stateCommitment,
			resubTimeoutMs: 30_000,
		},
		env: driftEnv,
	});

	const slotSubscriber = new SlotSubscriber(connection, {});

	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`DriftClient ProgramId: ${driftClient.program.programId.toBase58()}`
	);
	logger.info(`Wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(` . SOL balance: ${lamportsBalance / 10 ** 9}`);

	await driftClient.subscribe();
	driftClient.eventEmitter.on('error', (e) => {
		logger.error(e);
	});

	await slotSubscriber.subscribe();

	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	await redisClient.connect();

	const eventSubscriber = new EventSubscriber(connection, driftClient.program, {
		maxTx: 8192,
		maxEventsPerType: 4096,
		orderBy: 'client',
		commitment: 'confirmed',
		logProviderConfig: {
			type: 'polling',
			frequency: 1000,
		},
	});

	await eventSubscriber.subscribe();

	const eventObservable = fromEvent(eventSubscriber.eventEmitter, 'newEvent');
	eventObservable
		.pipe(
			filter(
				(event) =>
					event.eventType === 'OrderActionRecord' &&
					JSON.stringify(event.action) === JSON.stringify(OrderAction.FILL)
			),
			map((fill: Event<OrderActionRecord>) => {
				const basePrecision =
					getVariant(fill.marketType) === 'spot'
						? sdkConfig.SPOT_MARKETS[fill.marketIndex].precision
						: BASE_PRECISION;
				return {
					ts: fill.ts.toNumber(),
					marketIndex: fill.marketIndex,
					marketType: getVariant(fill.marketType),
					filler: fill.filler?.toBase58(),
					takerFee: convertToNumber(fill.takerFee, QUOTE_PRECISION),
					makerFee: convertToNumber(fill.makerFee, QUOTE_PRECISION),
					quoteAssetAmountSurplus: convertToNumber(
						fill.quoteAssetAmountSurplus,
						QUOTE_PRECISION
					),
					baseAssetAmountFilled: convertToNumber(
						fill.baseAssetAmountFilled,
						basePrecision
					),
					quoteAssetAmountFilled: convertToNumber(
						fill.quoteAssetAmountFilled,
						QUOTE_PRECISION
					),
					taker: fill.taker?.toBase58(),
					takerOrderId: fill.takerOrderId,
					takerOrderDirection: fill.takerOrderDirection
						? getVariant(fill.takerOrderDirection)
						: undefined,
					takerOrderBaseAssetAmount: convertToNumber(
						fill.takerOrderBaseAssetAmount,
						basePrecision
					),
					takerOrderCumulativeBaseAssetAmountFilled: convertToNumber(
						fill.takerOrderCumulativeBaseAssetAmountFilled,
						basePrecision
					),
					takerOrderCumulativeQuoteAssetAmountFilled: convertToNumber(
						fill.takerOrderCumulativeQuoteAssetAmountFilled,
						QUOTE_PRECISION
					),
					maker: fill.maker?.toBase58(),
					makerOrderId: fill.makerOrderId,
					makerOrderDirection: fill.makerOrderDirection
						? getVariant(fill.makerOrderDirection)
						: undefined,
					makerOrderBaseAssetAmount: convertToNumber(
						fill.makerOrderBaseAssetAmount,
						basePrecision
					),
					makerOrderCumulativeBaseAssetAmountFilled: convertToNumber(
						fill.makerOrderCumulativeBaseAssetAmountFilled,
						basePrecision
					),
					makerOrderCumulativeQuoteAssetAmountFilled: convertToNumber(
						fill.makerOrderCumulativeQuoteAssetAmountFilled,
						QUOTE_PRECISION
					),
					oraclePrice: convertToNumber(fill.oraclePrice, PRICE_PRECISION),
					txSig: fill.txSig,
					slot: fill.slot,
					fillRecordId: fill.fillRecordId?.toNumber(),
					action: 'fill',
					actionExplanation: getVariant(fill.actionExplanation),
					referrerReward: convertToNumber(
						new BN(fill.referrerReward ?? ZERO),
						QUOTE_PRECISION
					),
				};
			})
		)
		.subscribe((fillEvent) => {
			redisClient.client.publish(
				`trades_${fillEvent.marketType}_${fillEvent.marketIndex}`,
				JSON.stringify(fillEvent)
			);
		});

	console.log('Publishing trades');
};

async function recursiveTryCatch(f: () => void) {
	try {
		await f();
	} catch (e) {
		console.error(e);
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => main());

export { sdkConfig, endpoint, wsEndpoint, driftEnv, commitHash, driftClient };
