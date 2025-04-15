require('dotenv').config();  // Load environment variables from .env file
const { Client } = require('pg');

// PostgreSQL client setup using the DATABASE_URL from the .env file
const db = new Client({
  connectionString: process.env.DATABASE_URL,  // Use the DATABASE_URL environment variable
});

// Connect to DB
async function connectDb() {
  try {
    await db.connect();
    console.log('Connected to TimescaleDB');
  } catch (err) {
    console.error('Error connecting to TimescaleDB:', err);
    process.exit(1);
  }
}

// Utility function to execute queries (for INSERT, SELECT, UPDATE, DELETE)
async function queryDb(query, values = []) {
  try {
    const result = await db.query(query, values);
    return result;
  } catch (err) {
    console.error('Error executing query:', err);
    throw err;
  }
}

// Utility function to close the DB connection (when you're done)
async function closeDb() {
  try {
    await db.end();
    console.log('Disconnected from TimescaleDB');
  } catch (err) {
    console.error('Error closing connection:', err);
  }
}

module.exports = { connectDb, queryDb, closeDb };
