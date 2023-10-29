import WebSocket from 'ws';
const ws = new WebSocket('ws://localhost:3000/ws');

ws.on('open', () => {
  console.log('Connected to the server');
  ws.send(JSON.stringify({ type: 'subscribe', channel: 'SOL-PERP' }));
});

ws.on('message', (data: WebSocket.Data) => {
  try {
    const message = JSON.parse(data.toString());
    if (message.channel === 'SOL-PERP') {
      console.log('Received data:', message.data);
    }
  } catch (e) {
    console.error('Invalid message:', data);
  }
});

ws.on('close', () => {
  console.log('Disconnected from the server');
});

ws.on('error', (error: Error) => {
  console.error('WebSocket error:', error);
});
