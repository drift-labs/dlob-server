import cors from 'cors';
import express from 'express';
import * as http from 'http';
import compression from 'compression';
import { WebSocket, WebSocketServer } from 'ws';
import { sleep } from './utils/utils';
import { RedisClient } from './utils/redisClient';
import { register, Gauge } from 'prom-client';
import { DriftEnv, PerpMarkets, SpotMarkets } from '@drift-labs/sdk';

// Set up env constants
require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);

const wsConnectionsGauge = new Gauge({
	name: 'websocket_connections',
	help: 'Number of active WebSocket connections',
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const WS_PORT = process.env.WS_PORT || '3000';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

const getRedisChannelFromMessage = (message: any): string => {
	const channel = message.channel;
	const marketName = message.market?.toUpperCase();
	const marketType = message.marketType?.toLowerCase();
	if (!['spot', 'perp'].includes(marketType)) {
		throw new Error('Bad market type specified');
	}

	let marketIndex: number;
	if (marketType === 'spot') {
		marketIndex = SpotMarkets[driftEnv].find(
			(market) => market.symbol.toUpperCase() === marketName
		).marketIndex;
	} else if (marketType === 'perp') {
		marketIndex = PerpMarkets[driftEnv].find(
			(market) => market.symbol.toUpperCase() === marketName
		).marketIndex;
	}

	if (marketIndex === undefined || marketIndex === null) {
		throw new Error('Bad market specified');
	}

	switch (channel) {
		case 'trades':
			return `trades_${marketType}_${marketIndex}`;
		case 'orderbook':
			return `orderbook_${marketType}_${marketIndex}`;
		case undefined:
		default:
			throw new Error('Bad channel specified');
	}
};

async function main() {
	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	const lastMessageRetriever = new RedisClient(
		REDIS_HOST,
		REDIS_PORT,
		REDIS_PASSWORD
	);

	await redisClient.connect();
	await lastMessageRetriever.connect();

	const channelSubscribers = new Map<string, Set<WebSocket>>();
	const subscribedChannels = new Set<string>();

	redisClient.client.on('connect', () => {
		subscribedChannels.forEach(async (channel) => {
			try {
				await redisClient.client.subscribe(channel);
			} catch (error) {
				console.error(`Error subscribing to ${channel}:`, error);
			}
		});
	});

	redisClient.client.on('message', (subscribedChannel, message) => {
		const subscribers = channelSubscribers.get(subscribedChannel);
		subscribers.forEach((ws) => {
			ws.send(JSON.stringify({ channel: subscribedChannel, data: message }));
		});

		// Save and persist last message
		lastMessageRetriever.client.set(
			`last_update_${subscribedChannel}`,
			message
		);
	});

	redisClient.client.on('error', (error) => {
		console.error('Redis client error:', error);
	});

	wss.on('connection', (ws: WebSocket) => {
		console.log('Client connected');
		wsConnectionsGauge.inc();

		ws.on('message', async (msg) => {
			let parsedMessage: any;
			let messageType: string;
			try {
				parsedMessage = JSON.parse(msg.toString());
				messageType = parsedMessage.type.toLowerCase();
			} catch (e) {
				return;
			}

			switch (messageType) {
				case 'subscribe': {
					let redisChannel: string;
					try {
						redisChannel = getRedisChannelFromMessage(parsedMessage);
					} catch (error) {
						ws.send(JSON.stringify({ error: error.message }));
						return;
					}

					if (!subscribedChannels.has(redisChannel)) {
						console.log('Trying to subscribe to channel', redisChannel);
						redisClient.client
							.subscribe(redisChannel)
							.then(() => {
								subscribedChannels.add(redisChannel);
							})
							.catch(() => {
								ws.send(
									JSON.stringify({
										redisChannel,
										error: `Error subscribing to channel: ${redisChannel}`,
									})
								);
								return;
							});
					}

					if (!channelSubscribers.get(redisChannel)) {
						const subscribers = new Set<WebSocket>();
						channelSubscribers.set(redisChannel, subscribers);
					}
					channelSubscribers.get(redisChannel).add(ws);

					// Fetch and send last message
					if (redisChannel.includes('orderbook')) {
						const lastMessage = await lastMessageRetriever.client.get(
							`last_update_${redisChannel}`
						);
						if (lastMessage !== null) {
							ws.send(JSON.stringify({ redisChannel, data: lastMessage }));
						}
					}
					break;
				}
				case 'unsubscribe': {
					let redisChannel: string;
					try {
						redisChannel = getRedisChannelFromMessage(parsedMessage);
					} catch (error) {
						ws.send(JSON.stringify({ error: error.message }));
						return;
					}
					const subscribers = channelSubscribers.get(redisChannel);
					if (subscribers) {
						channelSubscribers.get(redisChannel).delete(ws);
					}
					break;
				}
				case undefined:
				default:
					break;
			}
		});

		// Ping/pong connection timeout
		let pongTimeoutId;
		let isAlive = true;
		const pingIntervalId = setInterval(() => {
			isAlive = false;
			pongTimeoutId = setTimeout(() => {
				if (!isAlive) {
					console.log('Disconnecting because of ping/pong timeout');
					ws.terminate();
				}
			}, 5000); // 5 seconds to wait for a pong
			ws.ping();
		}, 30000);

		// Listen for pong messages
		ws.on('pong', () => {
			isAlive = true;
			clearTimeout(pongTimeoutId);
		});

		// Handle disconnection
		ws.on('close', () => {
			// Clear any existing intervals and timeouts
			clearInterval(pingIntervalId);
			clearTimeout(pongTimeoutId);
			channelSubscribers.forEach((subscribers, channel) => {
				if (subscribers.delete(ws) && subscribers.size === 0) {
					redisClient.client.unsubscribe(channel);
					channelSubscribers.delete(channel);
					subscribedChannels.delete(channel);
				}
			});
			wsConnectionsGauge.dec();
		});

		ws.on('error', (error) => {
			console.error('Socket error:', error);
		});
	});

	server.listen(WS_PORT, () => {
		console.log(`connection manager running on ${WS_PORT}`);
	});

	app.get('/metrics', async (req, res) => {
		res.set('Content-Type', register.contentType);
		res.end(await register.metrics());
	});

	server.on('error', (error) => {
		console.error('Server error:', error);
	});
}

process.on('unhandledRejection', (reason, promise) => {
	console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

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
