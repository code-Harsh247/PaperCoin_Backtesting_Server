# One image, two jobs (wired up in PaperCoin's deploy/compose.yaml):
#   ingester  node index.js     Binance depth snapshots -> orderbook_snapshots
#   replay    node backtest.js  WebSocket replay of those snapshots
FROM node:22-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY . .
USER node
CMD ["node", "backtest.js"]
