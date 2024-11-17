import { program } from 'commander';

import { Connection, Commitment, Keypair } from '@solana/web3.js';

import {
	DriftClient,
	initialize,
	DriftEnv,
	Wallet,
	BulkAccountLoader,
	getMarketsAndOraclesForSubscription,
} from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';

import { logger, setLogLevel } from '../utils/logger';
import { sleep } from '../utils/utils';
import express from 'express';
// import { handleHealthCheck } from '../core/metrics';
import { setGlobalDispatcher, Agent } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();
const stateCommitment: Commitment = 'confirmed';
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
const redisClientPrefix = RedisClientPrefix.DLOB_HELIUS;
const RPCPOOL_PERCENTILE = 500; // p5
// Set up express for health checks
const app = express();

//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });
let driftClient: DriftClient;

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

const token = process.env.TOKEN;
const endpoint = token
	? process.env.ENDPOINT + `/${token}`
	: process.env.ENDPOINT;
const wsEndpoint = process.env.WS_ENDPOINT;
const FEE_POLLING_FREQUENCY =
	parseInt(process.env.FEE_POLLING_FREQUENCY) || 5000;

logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

enum RpcEndpointProvider {
	HELIUS = 'helius',
	RPCPOOL = 'rpcpool',
}

class PriorityFeeSubscriber {
	endpoint: string;
	perpMarketPubkeys: { marketIndex: number; pubkey: string }[];
	spotMarketPubkeys: { marketIndex: number; pubkeys: string[] }[];
	redisClient: RedisClient;
	frequencyMs: number;
	rpcEndpointProvider: RpcEndpointProvider;

	constructor(config: {
		endpoint: string;
		redisClient: RedisClient;
		perpMarketPubkeys: { marketIndex: number; pubkey: string }[];
		spotMarketPubkeys: { marketIndex: number; pubkeys: string[] }[];
		frequencyMs?: number;
	}) {
		if (endpoint.includes('helius')) {
			this.rpcEndpointProvider = RpcEndpointProvider.HELIUS;
		} else if (endpoint.includes('rpcpool')) {
			this.rpcEndpointProvider = RpcEndpointProvider.RPCPOOL;
		} else {
			throw new Error(`Fella what are you doing, why are you using another RPC, we cannot process priority fees, got: ${endpoint}`);
		}
		this.endpoint = config.endpoint;
		this.perpMarketPubkeys = config.perpMarketPubkeys;
		this.spotMarketPubkeys = config.spotMarketPubkeys;
		this.redisClient = config.redisClient;
		this.frequencyMs = config.frequencyMs ?? FEE_POLLING_FREQUENCY;
	}

	async subscribe() {
		await this.fetchAndPushPriorityFees();
		setInterval(async () => {
			await this.fetchAndPushPriorityFees();
		}, this.frequencyMs);
	}

	async fetchAndPushPriorityFees() {
		if (this.rpcEndpointProvider === RpcEndpointProvider.HELIUS) {
			await this.fetchAndPushPriorityFeesHelius();
		} else if (this.rpcEndpointProvider === RpcEndpointProvider.RPCPOOL) {
			await this.fetchAndPushPriorityFeesRpcPool();
		} else {
			throw new Error(`Unknown RPC endpoint provider: ${this.rpcEndpointProvider}`);
		}
	}

	async fetchAndPushPriorityFeesHelius() {
		const [resultPerp, resultSpot] = await Promise.all([
			fetch(this.endpoint, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(
					this.perpMarketPubkeys.map((xx) => {
						return {
							jsonrpc: '2.0',
							id: xx.marketIndex.toString(),
							method: 'getPriorityFeeEstimate',
							params: [
								{
									accountKeys: [xx.pubkey],
									options: {
										includeAllPriorityFeeLevels: true,
									},
								},
							],
						};
					})
				),
			}),
			fetch(this.endpoint, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(
					this.spotMarketPubkeys.map((xx) => {
						return {
							jsonrpc: '2.0',
							id: (100 + xx.marketIndex).toString(),
							method: 'getPriorityFeeEstimate',
							params: [
								{
									accountKeys: xx.pubkeys,
									options: {
										includeAllPriorityFeeLevels: true,
									},
								},
							],
						};
					})
				),
			}),
		]);

		const [dataPerp, dataSpot] = await Promise.all([
			resultPerp.json(),
			resultSpot.json(),
		]);

		dataPerp.forEach((result: any) => {
			const marketIndex = parseInt(result['id']);
			console.log(`perp market: ${marketIndex}`);
			console.log(result.result);
			this.redisClient.publish(
				`${redisClientPrefix}priorityFees_perp_${marketIndex}`,
				result.result['priorityFeeLevels']
			);
			this.redisClient.set(
				`priorityFees_perp_${marketIndex}`,
				result.result['priorityFeeLevels']
			);
		});

		dataSpot.forEach((result: any) => {
			const marketIndex = parseInt(result['id']) - 100;
			console.log(`spot market: ${marketIndex}`);
			console.log(result.result);
			this.redisClient.publish(
				`${redisClientPrefix}priorityFees_spot_${marketIndex}`,
				result.result['priorityFeeLevels']
			);
			this.redisClient.set(
				`priorityFees_spot_${marketIndex}`,
				result.result['priorityFeeLevels']
			);
		});
	}

	// Processes a result from getRecentPrioritizationFees and formats it to helius' priorityFeeLevels format.
	// RPCPool gives us an array of percentile observations at each slot (over 150 slots), we use the max over all slots as the priorityFeeLevels.
	//
	// @param result: is with ascending slots: [{"prioritizationFee": 1163819,"slot": 301026956},{"prioritizationFee": 221862,"slot": 301026957}, { "prioritizationFee": 0, "slot": 301026958 }, ...]
	processRpcPoolResult(result: Array<{ prioritizationFee: number, slot: number }>): any {
		// "{\"min\":0,\"low\":0,\"medium\":500,\"high\":469622,\"veryHigh\":10539741,\"unsafeMax\":1681742424}"
		// const value = result.reduce((max, current) => Math.max(max, current.prioritizationFee), 0);
		const value = Math.floor(result.reduce((sum, current) => sum + current.prioritizationFee, 0) / result.length);
		return {
			min: value,
			low: value,
			medium: value,
			high: value,
			veryHigh: value,
			unsafeMax: value,
		}
	}

	async fetchAndPushPriorityFeesRpcPool() {
		const [resultPerp, resultSpot] = await Promise.all([
			fetch(this.endpoint, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(
					this.perpMarketPubkeys.map((xx) => {
						return {
							jsonrpc: '2.0',
							id: xx.marketIndex.toString(),
							method: 'getRecentPrioritizationFees',
							params: [
								[xx.pubkey],
								// [],
								{
									percentile: RPCPOOL_PERCENTILE,
								},
							],
						};
					})
				),
			}),
			fetch(this.endpoint, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				body: JSON.stringify(
					this.spotMarketPubkeys.map((xx) => {
						return {
							jsonrpc: '2.0',
							id: (100 + xx.marketIndex).toString(),
							method: 'getRecentPrioritizationFees',
							params: [
								xx.pubkeys,
								// [],
								{
									percentile: RPCPOOL_PERCENTILE,
								},
							],
						};
					})
				),
			}),
		]);

		const [dataPerp, dataSpot] = await Promise.all([
			resultPerp.json(),
			resultSpot.json(),
		]);

		dataPerp.forEach((result: any) => {
			const marketIndex = parseInt(result['id']);
			const payload = this.processRpcPoolResult(result.result);
			console.log(`perp market: ${marketIndex}`);
			console.log(payload);
			this.redisClient.publish(
				`${redisClientPrefix}priorityFees_perp_${marketIndex}`,
				payload
			);
			this.redisClient.set(
				`priorityFees_perp_${marketIndex}`,
				payload
			);
		});

		dataSpot.forEach((result: any) => {
			const marketIndex = parseInt(result['id']) - 100;
			const payload = this.processRpcPoolResult(result.result);
			console.log(`spot market: ${marketIndex}`);
			console.log(payload);
			this.redisClient.publish(
				`${redisClientPrefix}priorityFees_spot_${marketIndex}`,
				payload
			);
			this.redisClient.set(
				`priorityFees_spot_${marketIndex}`,
				payload
			);
		});
	}
}

const main = async () => {
	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	const redisClient = new RedisClient({
		host: '127.0.0.1',
		port: '6379',

		prefix: redisClientPrefix,
	});
	await redisClient.connect();

	const { perpMarketIndexes, spotMarketIndexes, oracleInfos } =
		getMarketsAndOraclesForSubscription(sdkConfig.ENV);

	const driftClient = new DriftClient({
		connection,
		wallet: new Wallet(new Keypair()),
		perpMarketIndexes,
		spotMarketIndexes,
		oracleInfos,
		accountSubscription: {
			type: 'polling',
			accountLoader: new BulkAccountLoader(connection, stateCommitment, 0),
		},
	});
	await driftClient.subscribe();

	const perpMarketPubkeys = driftClient.getPerpMarketAccounts().map((acct) => {
		return { marketIndex: acct.marketIndex, pubkey: acct.pubkey.toString() };
	});

	const usdcMarket = driftClient.getSpotMarketAccount(0).pubkey.toString();
	const spotMarketPubkeys: { marketIndex: number; pubkeys: string[] }[] = [];
	for (const market of sdkConfig.SPOT_MARKETS) {
		const pubkeysForMarket = [usdcMarket];

		const driftMarket = driftClient.getSpotMarketAccount(market.marketIndex);
		pubkeysForMarket.push(driftMarket.pubkey.toString());

		if (market.serumMarket) {
			pubkeysForMarket.push(market.serumMarket.toString());
		}

		if (market.phoenixMarket) {
			pubkeysForMarket.push(market.phoenixMarket.toString());
		}

		if (market.openbookMarket) {
			pubkeysForMarket.push(market.openbookMarket.toString());
		}

		spotMarketPubkeys.push({
			marketIndex: market.marketIndex,
			pubkeys: pubkeysForMarket,
		});
	}

	const priorityFeeSubscriber = new PriorityFeeSubscriber({
		endpoint,
		perpMarketPubkeys,
		spotMarketPubkeys,
		redisClient,
	});

	await priorityFeeSubscriber.subscribe();
	const server = app.listen(8080);

	// Default keepalive is 5s, since the AWS ALB timeout is 60 seconds, clients
	// sometimes get 502s.
	// https://shuheikagawa.com/blog/2019/04/25/keep-alive-timeout/
	// https://stackoverflow.com/a/68922692
	server.keepAliveTimeout = 61 * 1000;
	server.headersTimeout = 65 * 1000;

	console.log('Priority fee publisher Publishing Messages');
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
