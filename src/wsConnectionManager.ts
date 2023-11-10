import cors from 'cors';
import express from 'express';
import * as http from 'http';
import compression from 'compression';
import { WebSocket, WebSocketServer } from 'ws';
import { sleep } from './utils/utils';
import { RedisClient } from './utils/redisClient';

// Set up env constants
require('dotenv').config();

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const WS_PORT = process.env.WS_PORT || '3000';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

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
					const channel = parsedMessage.channel;
					if (!subscribedChannels.has(channel)) {
						console.log('Trying to subscribe to channel', channel);
						redisClient.client
							.subscribe(channel)
							.then(() => {
								subscribedChannels.add(channel);
							})
							.catch(() => {
								ws.send(
									JSON.stringify({
										channel,
										error: `Error subscribing to channel: ${channel}`,
									})
								);
								return;
							});
					}

					if (!channelSubscribers.get(channel)) {
						const subscribers = new Set<WebSocket>();
						channelSubscribers.set(channel, subscribers);
					}
					channelSubscribers.get(channel).add(ws);

					// Fetch and send last message
					const lastMessage = await lastMessageRetriever.client.get(
						`last_update_${channel}`
					);
					if (lastMessage !== null) {
						ws.send(JSON.stringify({ channel, data: lastMessage }));
					}
					break;
				}
				case 'unsubscribe': {
					const channel = parsedMessage.channel;
					const subscribers = channelSubscribers.get(channel);
					if (subscribers) {
						channelSubscribers.get(channel).delete(ws);
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
		});

		ws.on('disconnect', () => {
			console.log('Client disconnected');
		});

		ws.on('error', (error) => {
			console.error('Socket error:', error);
		});
	});

	server.listen(WS_PORT, () => {
		console.log(`connection manager running on ${WS_PORT}`);
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
