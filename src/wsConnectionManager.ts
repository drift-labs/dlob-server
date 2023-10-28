import cors from 'cors';
import express from 'express';
import * as http from 'http';
import { Server } from 'socket.io';
import compression from 'compression';
import { sleep } from './utils/utils';
import { RedisClient } from './utils/redisClient';

const app = express();
app.use(cors({ origin: '*' }));
app.use(compression());
app.set('trust proxy', 1);

const server = http.createServer(app);
const io = new Server(server);

const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = process.env.REDIS_PORT || '6379';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;

async function main() {
	const redisClient = new RedisClient(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD);
	await redisClient.connect();

	io.on('connection', (socket) => {
		socket.on('subscribe', (channel) => {
			console.log('Subscribing to channel', channel);
			redisClient.client.subscribe(channel);
			redisClient.client.on('message', (subscribedChannel, message) => {
				if (subscribedChannel === channel) {
					socket.emit(channel, JSON.parse(message));
				}
			});
		});

		socket.on('unsubscribe', (channel) => {
			console.log('Unsubscribing from channel', channel);
			redisClient.client.unsubscribe(channel);
		});

		socket.on('disconnect', () => {
			console.log('Client disconnected');
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
