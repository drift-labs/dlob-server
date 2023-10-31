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
	await redisClient.connect();

	const channelSubscribers = new Map<string, Set<WebSocket>>();
	const subscribedChannels = new Set<string>();

	redisClient.client.on('message', (subscribedChannel, message) => {
		const subscribers = channelSubscribers.get(subscribedChannel);
		subscribers.forEach((ws) => {
			ws.send(JSON.stringify({ channel: subscribedChannel, data: message }));
		});
	});

	wss.on('connection', (ws: WebSocket) => {
		console.log('Client connected');

		ws.on('message', async (msg) => {
			const parsedMessage = JSON.parse(msg.toString());

			switch (parsedMessage.type.toLowerCase()) {
				case 'subscribe': {
					const channel = parsedMessage.channel;
					if (!subscribedChannels.has(channel)) {
						console.log('Subscribing to channel', channel);
						redisClient.client
							.subscribe(channel)
							.then(() => {
								subscribedChannels.add(channel);
							})
							.catch(() => {
								ws.send(
									JSON.stringify({
										channel,
										error: `Invalid channel: ${channel}`,
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
			console.log('poooooooong');
			isAlive = true;
			clearTimeout(pongTimeoutId);
		});

		// Handle disconnection
		ws.on('close', () => {
			// Clear any existing intervals and timeouts
			clearInterval(pingIntervalId);
			clearTimeout(pongTimeoutId);
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
}

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
