import cors from 'cors';
import express from 'express';
import * as http from 'http';
import compression from 'compression';
import { WebSocket, WebSocketServer } from 'ws';
import { sleep } from './utils/utils';
import { RedisClient } from './utils/redisClient';

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);

const server = http.createServer(app);
const wss = new WebSocketServer({server, path: '/ws'});

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

async function main() {
	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	await redisClient.connect();

	wss.on('connection', (ws: WebSocket) => {
		console.log('Client connected');

		ws.on('message', async (msg) => {
			const parsedMessage = JSON.parse(msg.toString());
			switch (parsedMessage.type) {
				case 'subscribe': {
					const channel = parsedMessage.channel;
					console.log('Subscribing to channel', channel);
					redisClient.client.subscribe(channel);
					redisClient.client.on('message', (subscribedChannel, message) => {
						if (subscribedChannel === channel) {
							ws.send(JSON.stringify({channel, data: message}));
						}
					});
					break;
				}
				case 'unsubscribe': {
					const channel = parsedMessage.channel;
					console.log('Unsubscribing from channel', channel);
					redisClient.client.unsubscribe(channel);
				}	
			}
		});

		// Ping/pong connection timeout
		let isAlive = true;

    ws.on('pong', () => {
      isAlive = true;
    });
		setInterval(() => {
			wss.clients.forEach((ws: WebSocket) => {
				if (isAlive === false) return ws.terminate();
		
				isAlive = false;
				ws.ping();
			});
		}, 30000);

		ws.on('disconnect', () => {
			console.log('Client disconnected');
		});

		ws.on('error', (error) => {
			console.error('Socket error:', error);
		});
	});

	server.listen('3000', () => {
		console.log('connection manager running on 3000');
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
