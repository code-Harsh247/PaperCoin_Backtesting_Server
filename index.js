require('dotenv').config();  // Load environment variables from .env file
const WebSocket = require('ws');
const { connectDb, queryDb, closeDb } = require('./util/db');

// Binance WebSocket URL for BTC order book (top 20 bids/asks)
const binanceWsUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth20@1000ms';

let shuttingDown = false;

// Connect to DB, then keep a Binance connection alive forever.
async function startIngestion() {
  await connectDb();
  console.log('Ingestion script started...');
  connect();
}

// Whole-connection setup lives in one function so a reconnect gets handlers
// too — the old code re-created the socket on close without re-attaching
// them, which silently stopped ingestion after the first drop.
function connect() {
  if (shuttingDown) return;
  const ws = new WebSocket(binanceWsUrl);

  ws.on('open', () => console.log('Connected to Binance depth stream'));

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      const timestamp = new Date();
      const { bids, asks } = message;

      if (bids && asks) {
        await insertSnapshot(bids, asks, timestamp);
      } else {
        console.log('Missing bids or asks in message:', message);
      }
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
    // 'close' follows 'error'; reconnection happens there.
  });

  ws.on('close', () => {
    if (shuttingDown) return;
    console.log('WebSocket connection closed. Reconnecting in 5s...');
    setTimeout(connect, 5000);
  });
}

// Function to insert order book snapshot into TimescaleDB
async function insertSnapshot(bids, asks, timestamp) {
  const query = `
    INSERT INTO orderbook_snapshots (timestamp, bids, asks)
    VALUES ($1, $2, $3)
  `;
  const values = [timestamp, JSON.stringify(bids), JSON.stringify(asks)];

  await queryDb(query, values);
}

// Start the ingestion process
startIngestion();

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Received SIGINT. Closing connection...');
  shuttingDown = true;
  await closeDb();
  process.exit(0);
});
