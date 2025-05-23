import cors from 'cors';
import express from 'express';
import * as http from 'http';
import compression from 'compression';
import { WebSocket, WebSocketServer } from 'ws';
import { sleep, selectMostRecentBySlot } from './utils/utils';
import { register, Gauge, Counter } from 'prom-client';
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
const wsOrderbookSourceCounter = new Counter({
	name: 'websocket_orderbook_source',
	help: 'Number of orderbook messages sent from source',
	labelNames: ['source'],
});
const wsOrderbookSourceLastSlotGauge = new Gauge({
	name: 'websocket_orderbook_source_last_slot',
	help: 'Last slot of orderbook messages from a source',
	labelNames: ['source'],
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

const envClients = [];
const clients = process.env.REDIS_CLIENT?.trim()
	.replace(/^\[|\]$/g, '')
	.split(/\s*,\s*/);

clients?.forEach((client) => envClients.push(RedisClientPrefix[client]));

const REDIS_CLIENTS = envClients.length
	? envClients
	: [RedisClientPrefix.DLOB, RedisClientPrefix.DLOB_HELIUS];
console.log('Redis Clients:', REDIS_CLIENTS);

const regexp = new RegExp(REDIS_CLIENTS.join('|'), 'g');
const sanitiseChannelForClient = (channel: string | undefined): string => {
	return channel?.replace(regexp, '');
};
const getChannelPrefix = (channel: string | undefined): string | undefined => {
	if (!channel) return undefined;
	const match = channel.match(regexp);
	return match?.[0];
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
			return `trades_${marketType}_${marketIndex}`;
		case 'orderbook':
			return `orderbook_${marketType}_${marketIndex}`;
		case 'orderbook_indicative':
			return `orderbook_${marketType}_${marketIndex}_indicative`;
		case 'orderbook_delta':
			return `orderbook_${marketType}_${marketIndex}_delta`;
		case 'priorityfees':
			return `priorityFees_${marketType}_${marketIndex}`;
		case undefined:
		default:
			throw new Error('Bad channel specified');
	}
};

async function main() {
	const subscribedChannelToSlot: Map<string, number> = new Map();

	const redisClients: Array<RedisClient> = [];
	const lastMessageClients: Array<RedisClient> = [];
	for (let i = 0; i < REDIS_CLIENTS.length; i++) {
		const redisClient = new RedisClient({ prefix: REDIS_CLIENTS[i] });
		const lastMessageClient = new RedisClient({ prefix: REDIS_CLIENTS[i] });
		await redisClient.connect();
		await lastMessageClient.connect();

		lastMessageClients.push(lastMessageClient);
		redisClients.push(redisClient);

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
			const sanitizedChannel = sanitiseChannelForClient(subscribedChannel);
			const channelPrefix = getChannelPrefix(sanitizedChannel);
			const subscribers = channelSubscribers.get(sanitizedChannel);
			if (subscribers) {
				if (sanitizedChannel.includes('orderbook')) {
					const messageSlot = JSON.parse(message)['slot'];
					wsOrderbookSourceLastSlotGauge.set(
						{
							source: channelPrefix,
						},
						messageSlot
					);

					const lastMessageSlot = subscribedChannelToSlot.get(sanitizedChannel);
					if (!lastMessageSlot || lastMessageSlot <= messageSlot) {
						subscribedChannelToSlot.set(sanitizedChannel, messageSlot);
					} else if (lastMessageSlot > messageSlot) {
						return;
					}
				}

				subscribers.forEach((ws) => {
					if (
						ws.readyState === WebSocket.OPEN &&
						ws.bufferedAmount < MAX_BUFFERED_AMOUNT
					) {
						wsOrderbookSourceCounter.inc({
							source: channelPrefix,
						});

						if (sanitizedChannel.includes('delta')) {
							ws.send(
								JSON.stringify({
									channel: sanitizedChannel,
									data: JSON.parse(message),
								})
							);
						} else {
							ws.send(
								JSON.stringify({
									channel: sanitizedChannel,
									data: message,
								})
							);
						}
					}
				});
			}
		});

		redisClient.forceGetClient().on('error', (error) => {
			console.error('Redis client error:', error);
		});
	}

	const channelSubscribers = new Map<string, Set<WebSocket>>();
	const subscribedChannels = new Set<string>();

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
						redisClients[0]
							.subscribe(
								redisChannel.includes('priorityFees')
									? `dlob-helius:${redisChannel}`
									: `${redisClients[0].getPrefix()}${redisChannel}`
							)
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

						if (redisChannel.includes('orderbook') && redisClients.length > 1) {
							redisClients.slice(1).map((redisClient) => {
								redisClient
									.subscribe(`${redisClient.getPrefix()}${redisChannel}`)
									.catch(() => {
										ws.send(
											JSON.stringify({
												error: `Error subscribing to channel: ${parsedMessage}`,
											})
										);
										return;
									});
							});
						}
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

					if (redisChannel.includes('delta')) {
						const lastMessages = await Promise.all(
							lastMessageClients.map((redisClient) =>
								redisClient.getRaw(redisChannel.replace('delta', 'snapshot'))
							)
						);
						const lastMessage = selectMostRecentBySlot(lastMessages);
						if (lastMessage) {
							ws.send(
								JSON.stringify({ channel: redisChannel, data: lastMessage })
							);
						}
					} else if (redisChannel.includes('orderbook')) {
						const lastMessages = await Promise.all(
							lastMessageClients.map((redisClient) =>
								redisClient.getRaw(
									`last_update_${redisChannel}`.replace(
										redisClient.getPrefix(),
										''
									)
								)
							)
						);
						const lastMessage = selectMostRecentBySlot(lastMessages);
						if (lastMessage) {
							ws.send(
								JSON.stringify({
									channel: redisChannel,
									data: JSON.stringify(lastMessage),
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

				case 'snapshot': {
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
									error: 'Error retrieving snapshots',
								})
							);
						} else {
							ws.close(
								1003,
								JSON.stringify({
									error: 'Error retrieving snapshots',
								})
							);
						}
						return;
					}

					try {
						const snapshots = await Promise.all(
							lastMessageClients.map((redisClient) =>
								redisClient.getRaw(redisChannel.replace('delta', 'snapshot'))
							)
						);
	
						const snapshot = selectMostRecentBySlot(snapshots);
	
						if (snapshot) {
							ws.send(
								JSON.stringify({
									channel: redisChannel,
									data: snapshot,
								})
							);
						}
					} catch(error) {
						console.error(error)
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
					redisClients.map((redisClient) => redisClient.unsubscribe(channel));
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
