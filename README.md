# PaperCoin Backtesting Server (display-only)

Two small services around the `orderbook_snapshots` table:

- `index.js` — ingester. Streams Binance BTCUSDT depth (top 20, 1s) into
  `orderbook_snapshots`. Run it continuously; history only exists from the
  moment this starts.
- `backtest.js` — replay server. A WebSocket on `BACKTEST_PORT` (default
  8080) that streams a `[startDate, endDate]` window of snapshots, paced by
  their real timestamp gaps divided by `speed`.

```bash
cp .env.example .env   # then fill in DATABASE_URL
npm install
npm run ingest         # capture (keep running; pm2 works: pm2 start index.js)
npm run serve          # replay server
```

Client protocol: connect, then send `{"startDate", "endDate", "speed"}`.
The server answers `{status:'started', totalSnapshots, speed}` then a message
per snapshot `{timestamp, bids, asks, progress}` and finally
`{status:'completed'}`.

## Role in the system — read this before extending

This server is **display-only**. It animates what the order book *looked
like*; the main app's trading engine (`PaperCoin/engine/backtest`) decides
what *filled*, from captured trades, using the same fill model as live
trading. A book snapshot says what was quoted, not what traded — do not add
fill logic here.
