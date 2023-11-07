import WebSocket from 'ws';
const ws = new WebSocket('wss://master.dlob.drift.trade/ws');
import { sleep } from '../src/utils/utils';

ws.on('open', async () => {
  console.log('Connected to the server');
  ws.send(JSON.stringify({ type: 'subscribe', channel: 'SOL-PERP' }));
  ws.send(JSON.stringify({ type: 'subscribe', channel: 'LINK-PERP' }));
  ws.send(JSON.stringify({ type: 'subscribe', channel: 'INJ-PERP' }));
  await sleep(5000);
  
  ws.send(JSON.stringify({ type: 'unsubscribe', channel: 'SOL-PERP' }));
  console.log("####################");
});

ws.on('message', (data: WebSocket.Data) => {
  try {
    const message = JSON.parse(data.toString());
    console.log(`Received data from market ${message.channel}`);
    // book data is in message.data
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
