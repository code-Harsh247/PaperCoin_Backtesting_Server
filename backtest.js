require('dotenv').config();
const WebSocket = require('ws');
const { connectDb, queryDb, closeDb } = require('./util/db');

// DISPLAY-ONLY SERVER. This replays captured order-book snapshots so the UI
// can animate historical depth. It does not decide fills — the trading
// engine's backtest module (PaperCoin/engine/backtest) owns those, driven by
// captured trades. Keep it that way: a book snapshot tells you what was
// quoted, not what traded.

const PORT = process.env.BACKTEST_PORT || 8080;
const MAX_SPEED = 10;

const wss = new WebSocket.Server({ port: PORT });
console.log(`Backtesting WebSocket server started on port ${PORT}`);

wss.on('connection', async (ws) => {
  console.log('Client connected to backtesting server');

  // One replay per connection at a time; the flag also stops the loop the
  // moment the client goes away.
  let streaming = false;

  ws.on('message', async (message) => {
    try {
      const config = JSON.parse(message);
      console.log('Received backtesting configuration:', config);

      const { startDate, endDate } = config;
      if (!startDate || !endDate) {
        ws.send(JSON.stringify({ error: 'Start date and end date are required' }));
        return;
      }
      if (streaming) {
        ws.send(JSON.stringify({ error: 'A session is already streaming on this connection' }));
        return;
      }

      // Clamp speed to something sane; default to real time.
      const speed = Math.min(Math.max(Number(config.speed) || 1, 0.1), MAX_SPEED);

      streaming = true;
      try {
        await streamHistoricalData(ws, startDate, endDate, speed, () => streaming);
      } finally {
        streaming = false;
      }
    } catch (error) {
      console.error('Error processing client message:', error);
      ws.send(JSON.stringify({ error: 'Invalid configuration format' }));
    }
  });

  ws.send(JSON.stringify({
    status: 'connected',
    message: 'Connected to backtesting server. Send configuration to begin.',
  }));

  ws.on('close', () => {
    streaming = false;
    console.log('Client disconnected from backtesting server');
  });
});

// Streams snapshots paced by their real timestamp gaps divided by `speed`,
// so 5x plays an hour of history in twelve minutes rather than emitting one
// frame per second regardless of how the data was spaced.
async function streamHistoricalData(ws, startDate, endDate, speed, isActive) {
  try {
    const query = `
      SELECT timestamp, bids, asks
      FROM orderbook_snapshots
      WHERE timestamp BETWEEN $1 AND $2
      ORDER BY timestamp ASC
    `;

    const startTimestamp = new Date(startDate);
    const endTimestamp = new Date(endDate);
    console.log(`Fetching data from ${startTimestamp} to ${endTimestamp} at ${speed}x`);

    const result = await queryDb(query, [startTimestamp, endTimestamp]);

    if (result.rows.length === 0) {
      ws.send(JSON.stringify({
        status: 'error',
        message: 'No data found for the specified date range',
      }));
      return;
    }

    ws.send(JSON.stringify({
      status: 'started',
      totalSnapshots: result.rows.length,
      startTime: startTimestamp,
      endTime: endTimestamp,
      speed,
    }));

    let prevTs = null;
    for (let i = 0; i < result.rows.length; i++) {
      if (!isActive() || ws.readyState !== WebSocket.OPEN) {
        console.log('Client gone during streaming; stopping replay');
        break;
      }

      const snapshot = result.rows[i];
      const ts = new Date(snapshot.timestamp).getTime();
      if (prevTs !== null) {
        // Cap the wait so a capture gap (ingester downtime) doesn't stall the
        // replay for the same wall-clock duration as the outage.
        const wait = Math.min((ts - prevTs) / speed, 5000);
        if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
      }
      prevTs = ts;

      ws.send(JSON.stringify({
        timestamp: snapshot.timestamp,
        bids: snapshot.bids,
        asks: snapshot.asks,
        progress: `${i + 1}/${result.rows.length}`,
      }));
    }

    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        status: 'completed',
        message: 'Backtesting session completed',
      }));
    }
  } catch (error) {
    console.error('Error streaming historical data:', error);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        status: 'error',
        message: 'Error during data streaming',
      }));
    }
  }
}

// Fail loudly at boot if the database is unreachable.
connectDb();

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down backtesting server...');

  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ status: 'shutdown', message: 'Server shutting down' }));
      client.close();
    }
  });

  await closeDb();
  process.exit(0);
});
