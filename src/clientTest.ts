import { io } from 'socket.io-client';

const main = async () => {
	const socket = io('http://localhost:3000');

	socket.on('connect', () => {
		socket.emit('subscribe', 'SOL-PERP');
	});

	socket.on('SOL-PERP', (data: any) => {
		console.log(data);
	});
};

main().then(() => {
	console.log('running');
});
