require('dotenv').config();  // Load environment variables from .env file
const WebSocket = require('ws');
const { connectDb, queryDb, closeDb } = require('./util/db');

// Binance WebSocket URL for BTC order book (top 20 bids/asks)
const binanceWsUrl = 'wss://stream.binance.com:9443/ws/btcusdt@depth20@1000ms';

// Connect to DB
async function startIngestion() {
  await connectDb();
  console.log('Ingestion script started...');
  
  // Create WebSocket connection to Binance
  let ws = new WebSocket(binanceWsUrl);

  // Handle incoming WebSocket data
  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data);
      
      // Extract timestamp (note: message doesn't have timestamp, using current time)
      const timestamp = new Date();
      
      // Extract bids and asks directly from the message
      const bids = message.bids;
      const asks = message.asks;
      
      if (bids && asks) {
        // Insert into TimescaleDB
        await insertSnapshot(bids, asks, timestamp);
      } else {
        console.log('Missing bids or asks in message:', message);
      }
    } catch (err) {
      console.error('Error processing message:', err);
    }
  });

  // Handle WebSocket errors
  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });

  // Handle WebSocket connection closure
  ws.on('close', () => {
    console.log('WebSocket connection closed. Reconnecting...');
    setTimeout(() => {
      ws = new WebSocket(binanceWsUrl);  // Reconnect
    }, 5000);
  });
}

// Function to insert order book snapshot into TimescaleDB
async function insertSnapshot(bids, asks, timestamp) {
  const query = `
    INSERT INTO orderbook_snapshots (timestamp, bids, asks)
    VALUES ($1, $2, $3)
  `;
  const values = [timestamp, JSON.stringify(bids), JSON.stringify(asks)];
  
  await queryDb(query, values);  // Using the query utility
  console.log(`Inserted snapshot at ${timestamp}`);
}

// Start the ingestion process
startIngestion();

// Graceful shutdown (optional)
process.on('SIGINT', async () => {
  console.log('Received SIGINT. Closing connection...');
  await closeDb();
  process.exit(0);
});
