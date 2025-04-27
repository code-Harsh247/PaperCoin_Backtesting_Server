require('dotenv').config();
const WebSocket = require('ws');
const { connectDb, queryDb, closeDb } = require('./util/db');

// Create WebSocket server for backtesting
const PORT = process.env.BACKTEST_PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

console.log(`Backtesting WebSocket server started on port ${PORT}`);

// Handle client connections
wss.on('connection', async (ws) => {
  console.log('Client connected to backtesting server');
  
  // Handle messages from client (configuration requests)
  ws.on('message', async (message) => {
    try {
      const config = JSON.parse(message);
      console.log('Received backtesting configuration:', config);
      
      // Extract configuration parameters
      const { startDate, endDate } = config;
      
      // Validate configuration
      if (!startDate || !endDate) {
        ws.send(JSON.stringify({ error: 'Start date and end date are required' }));
        return;
      }
      
      // Start the backtesting session
      await streamHistoricalData(ws, startDate, endDate);
      
    } catch (error) {
      console.error('Error processing client message:', error);
      ws.send(JSON.stringify({ error: 'Invalid configuration format' }));
    }
  });
  
  // Send initial connection confirmation
  ws.send(JSON.stringify({ 
    status: 'connected',
    message: 'Connected to backtesting server. Send configuration to begin.'
  }));
  
  // Handle client disconnection
  ws.on('close', () => {
    console.log('Client disconnected from backtesting server');
    // Clean up any active streaming (if needed)
  });
});

// Function to stream historical orderbook data to client
async function streamHistoricalData(ws, startDate, endDate) {
  try {
    // Connect to database (if not already connected)
    await connectDb();
    
    // Query to get data within date range, ordered by timestamp
    const query = `
      SELECT timestamp, bids, asks 
      FROM orderbook_snapshots 
      WHERE timestamp BETWEEN $1 AND $2 
      ORDER BY timestamp ASC
    `;
    
    const startTimestamp = new Date(startDate);
    const endTimestamp = new Date(endDate);
    
    console.log(`Fetching data from ${startTimestamp} to ${endTimestamp}`);
    
    // Execute query
    const result = await queryDb(query, [startTimestamp, endTimestamp]);
    
    if (result.rows.length === 0) {
      ws.send(JSON.stringify({ 
        status: 'error', 
        message: 'No data found for the specified date range' 
      }));
      return;
    }
    
    // Send metadata about the backtesting session
    ws.send(JSON.stringify({
      status: 'started',
      totalSnapshots: result.rows.length,
      startTime: startTimestamp,
      endTime: endTimestamp
    }));
    
    // Process and send each snapshot at 1 per second
    for (let i = 0; i < result.rows.length; i++) {
      const snapshot = result.rows[i];
      
      // Check if client is still connected
      if (ws.readyState !== WebSocket.OPEN) {
        console.log('Client disconnected during streaming');
        break;
      }
      
      // Send the snapshot to the client
      ws.send(JSON.stringify({
        timestamp: snapshot.timestamp,
        bids: snapshot.bids,  
        asks: snapshot.asks,  
        progress: `${i + 1}/${result.rows.length}`
      }));
      
      // Wait 1 second before sending the next snapshot
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Send completion message
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        status: 'completed',
        message: 'Backtesting session completed'
      }));
    }
    
  } catch (error) {
    console.error('Error streaming historical data:', error);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ 
        status: 'error', 
        message: 'Error during data streaming' 
      }));
    }
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down backtesting server...');
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ status: 'shutdown', message: 'Server shutting down' }));
      client.close();
    }
  });
  
  await closeDb();
  process.exit(0);
});