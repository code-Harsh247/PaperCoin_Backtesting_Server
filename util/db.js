require('dotenv').config();  // Load environment variables from .env file
const { Pool } = require('pg');

// A Pool, not a single Client: the backtest server handles concurrent
// WebSocket sessions, and a shared Client crashes the moment a second
// connection calls connect() on it. The pool hands each query a free
// connection and manages lifecycles itself — connectDb() is kept only as a
// startup ping so misconfiguration fails loudly at boot instead of on the
// first client.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
});

async function connectDb() {
  try {
    const client = await pool.connect();
    client.release();
    console.log('Connected to TimescaleDB');
  } catch (err) {
    console.error('Error connecting to TimescaleDB:', err);
    process.exit(1);
  }
}

// Utility function to execute queries (for INSERT, SELECT, UPDATE, DELETE)
async function queryDb(query, values = []) {
  try {
    return await pool.query(query, values);
  } catch (err) {
    console.error('Error executing query:', err);
    throw err;
  }
}

async function closeDb() {
  try {
    await pool.end();
    console.log('Disconnected from TimescaleDB');
  } catch (err) {
    console.error('Error closing connection:', err);
  }
}

module.exports = { connectDb, queryDb, closeDb };
