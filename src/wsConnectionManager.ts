import cors from 'cors';
import express from 'express';
import * as http from 'http';
import compression from 'compression';
import { WebSocket, WebSocketServer } from 'ws';
import { sleep } from './utils/utils';
import { register, Gauge } from 'prom-client';
import { DriftEnv, PerpMarkets, SpotMarkets } from '@drift-labs/sdk';
import { RedisClient, RedisClientPrefix } from '@drift/common/clients';

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
const wss = new WebSocketServer({
	server,
	path: '/ws',
	perMessageDeflate: true,
});

const WS_PORT = process.env.WS_PORT || '3000';

console.log(`WS LISTENER PORT : ${WS_PORT}`);

const MAX_BUFFERED_AMOUNT = 300000;
const CHANNEL_PREFIX = RedisClientPrefix.DLOB;
const CHANNEL_PREFIX_HELIUS = RedisClientPrefix.DLOB_HELIUS;

const sanitiseChannelForClient = (channel: string | undefined): string => {
	return channel
		?.replace(CHANNEL_PREFIX, '')
		?.replace(CHANNEL_PREFIX_HELIUS, '');
};

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

	switch (channel.toLowerCase()) {
		case 'trades':
			return `${CHANNEL_PREFIX}trades_${marketType}_${marketIndex}`;
		case 'orderbook':
			return `${CHANNEL_PREFIX}orderbook_${marketType}_${marketIndex}`;
		case 'priorityfees':
			return `${CHANNEL_PREFIX_HELIUS}priorityFees_${marketType}_${marketIndex}`;
		case undefined:
		default:
			throw new Error('Bad channel specified');
	}
};

async function main() {
	const redisClient = new RedisClient({});
	const lastMessageRetriever = new RedisClient({ prefix: CHANNEL_PREFIX });

	await redisClient.connect();
	await lastMessageRetriever.connect();

	const channelSubscribers = new Map<string, Set<WebSocket>>();
	const subscribedChannels = new Set<string>();

	redisClient.forceGetClient().on('connect', () => {
		subscribedChannels.forEach(async (channel) => {
			try {
				await redisClient.subscribe(channel);
			} catch (error) {
				console.error(`Error subscribing to ${channel}:`, error);
			}
		});
	});

	redisClient.forceGetClient().on('message', (subscribedChannel, message) => {
		const subscribers = channelSubscribers.get(subscribedChannel);
		if (subscribers) {
			subscribers.forEach((ws) => {
				if (
					ws.readyState === WebSocket.OPEN &&
					ws.bufferedAmount < MAX_BUFFERED_AMOUNT
				)
					ws.send(
						JSON.stringify({
							channel: sanitiseChannelForClient(subscribedChannel),
							data: message,
						})
					);
			});
		}
	});

	redisClient.forceGetClient().on('error', (error) => {
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
						const requestChannel = sanitiseChannelForClient(
							parsedMessage?.channel
						);
						if (requestChannel) {
							ws.send(
								JSON.stringify({
									channel: requestChannel,
									error: 'Error subscribing to channel',
								})
							);
						} else {
							ws.close(
								1003,
								JSON.stringify({
									error: 'Error subscribing to channel',
								})
							);
						}
						return;
					}

					if (!subscribedChannels.has(redisChannel)) {
						console.log('Trying to subscribe to channel', redisChannel);
						redisClient
							.subscribe(redisChannel)
							.then(() => {
								subscribedChannels.add(redisChannel);
							})
							.catch(() => {
								ws.send(
									JSON.stringify({
										error: `Error subscribing to channel: ${parsedMessage}`,
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

					ws.send(
						JSON.stringify({
							message: `Subscribe received for channel: ${parsedMessage.channel}, market: ${parsedMessage.market}, marketType: ${parsedMessage.marketType}`,
						})
					);
					// Fetch and send last message
					if (redisChannel.includes('orderbook')) {
						const lastUpdateChannel = `last_update_${redisChannel}`.replace(
							CHANNEL_PREFIX,
							''
						);
						const lastMessage = await lastMessageRetriever.getRaw(
							lastUpdateChannel
						);
						if (lastMessage) {
							ws.send(
								JSON.stringify({
									channel: lastUpdateChannel,
									data: lastMessage,
								})
							);
						}
					}

					break;
				}
				case 'unsubscribe': {
					let redisChannel: string;
					try {
						redisChannel = getRedisChannelFromMessage(parsedMessage);
					} catch (error) {
						const requestChannel = sanitiseChannelForClient(
							parsedMessage?.channel
						);
						if (requestChannel) {
							console.log('Error unsubscribing from channel:', error.message);
							ws.send(
								JSON.stringify({
									channel: requestChannel,
									error: 'Error unsubscribing from channel',
								})
							);
						} else {
							ws.close(
								1003,
								JSON.stringify({
									error: 'Error unsubscribing from channel',
								})
							);
						}
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

		// Set interval to send heartbeat every 5 seconds
		const heartbeatInterval = setInterval(() => {
			if (ws.readyState === WebSocket.OPEN) {
				ws.send(JSON.stringify({ channel: 'heartbeat' }));
			} else {
				clearInterval(heartbeatInterval);
			}
		}, 5000);

		// Buffer overflow check interval
		const bufferInterval = setInterval(() => {
			if (ws.bufferedAmount > MAX_BUFFERED_AMOUNT) {
				if (ws.readyState === WebSocket.OPEN) {
					ws.close(1008, 'Buffer overflow');
				}
				clearInterval(bufferInterval);
			}
		}, 10000);

		// Handle disconnection
		ws.on('close', () => {
			console.log('Client disconnected');
			// Clear any existing intervals
			clearInterval(heartbeatInterval);
			clearInterval(bufferInterval);
			channelSubscribers.forEach((subscribers, channel) => {
				if (subscribers.delete(ws) && subscribers.size === 0) {
					redisClient.unsubscribe(channel);
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
