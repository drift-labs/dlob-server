import Redis, { RedisOptions } from 'ioredis';
import { sleep } from './utils';

export const getRedisClient = (
	host?: string,
	port?: string,
	password?: string,
	db?: number,
	opts?: RedisOptions
): Redis => {
	if (host && port) {
		console.log(`Connecting to configured redis:: ${host}:${port}`);

		const redisClient = new Redis({
			host: host,
			port: parseInt(port, 10),
			password: password,
			...(opts ?? {}),
			retryStrategy: (times) => {
				const delay = Math.min(times * 1000, 10000);
				console.log(
					`Reconnecting to Redis in ${delay}ms... (retries: ${times})`
				);
				return delay;
			},
			reconnectOnError: (err) => {
				const targetError = 'ECONNREFUSED';
				if (err.message.includes(targetError)) {
					console.log(
						`Redis error: ${targetError}. Attempting to reconnect...`
					);
					return true;
				}
				return false;
			},
			maxRetriesPerRequest: null, // unlimited retries
		});

		redisClient.on('connect', () => {
			console.log('Connected to Redis.');
		});

		redisClient.on('error', (err) => {
			console.error('Redis error:', err);
		});

		redisClient.on('reconnecting', () => {
			console.log('Reconnecting to Redis...');
		});

		return redisClient;
	}

	console.log(`Using default redis`);
	return new Redis({ db });
};

/**
 * Wrapper around the redis client.
 *
 * You can hover over the underlying redis client methods for explanations of the methods, but will also include links to DOCS for some important concepts below:
 *
 * zRange, zRangeByScore etc.:
 * - All of the "z" methods are methods that use sorted sets.
 * - Sorted sets are explained here : https://redis.io/docs/data-types/sorted-sets/
 */
export class RedisClient {
	public client: Redis;

	connectionPromise: Promise<void>;

	constructor(
		host?: string,
		port?: string,
		password?: string,
		db?: number,
		opts?: RedisOptions
	) {
		this.client = getRedisClient(host, port, password, db, opts);
	}

	/**
	 * Should avoid using this unless necessary.
	 * @returns
	 */
	public forceGetClient() {
		return this.client;
	}

	public get connected() {
		return this?.client?.status === 'ready';
	}

	async connect() {
		if (this.client.status === 'ready' || this.client.status === 'connect') {
			return;
		}

		if (this.client.status === 'connecting') {
			await sleep(100);
			return this.connect(); // recursive call to check again
		}

		try {
			await this.client.connect();
		} catch (e) {
			console.error(e);
		}
	}

	disconnect() {
		this.client.disconnect();
	}

	private assertConnected() {
		if (!this.connected) {
			throw 'Redis client not connected';
		}
	}

	private getPrefixForMetrics(key: string) {
		return key.split(':')?.[0];
	}
}
