# Solana Worker with Auto-Rotating RPC Pool (Railway Deployment)

A lightweight Go worker optimized for deployment on **Railway** (or any container hosting platform) with intelligent RPC auto-rotation and circuit-breaker rate-limit protection.

---

## ⚡ Features

- **High-Throughput Batch Checking**: Queries up to 100 addresses per single RPC request using Solana's `getMultipleAccounts` JSON-RPC method with minimal payload overhead (`dataSlice: { offset: 0, length: 0 }`).
- **Smart RPC Auto-Rotation**: Distributes queries across a pool of public & custom Solana RPC endpoints.
- **Circuit-Breaker & Rate-Limit Backoff**:
  - Automatically detects HTTP 429 (Too Many Requests), 5xx errors, timeouts, or RPC rate-limit errors.
  - Temporarily puts the affected RPC into a cooldown state (30s, 60s, 120s exponential backoff).
  - Automatically retries the query on the next available healthy RPC.
- **Dynamic Custom RPCs**: Add private RPCs (Helius, QuickNode, Alchemy) via the `CUSTOM_RPCS` environment variable.
- **Live Status & Health Check**: Access the root URL (`/`) to see real-time uptime, stats, and the health status of every RPC in the pool.
- **Ultra-Lightweight**: Multi-stage Alpine container consuming < 15MB RAM.

---

## 📁 Project Structure

```
├── main.go            # Application logic with smart RPC pool & health check
├── Dockerfile         # Multi-stage build (< 15MB memory footprint)
├── railway.json       # Railway deployment configuration
├── go.mod             # Go module definition
├── go.sum             # Dependency checksums
├── .env.example       # Example environment variables
├── .dockerignore      # Docker ignore rules
└── .gitignore         # Git ignore rules
```

---

## 🚀 How to Deploy to Railway

### Method 1: Deploy via GitHub (Recommended)

1. **Initialize Git & Push to GitHub**:
   ```bash
   git init
   git add .
   git commit -m "Solana worker with RPC auto-rotation"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
   git push -u origin main
   ```

2. **Deploy on Railway**:
   - Go to [railway.app](https://railway.app) and sign in.
   - Click **+ New Project** -> **Deploy from GitHub repo**.
   - Select your repository.

3. **Configure Environment Variables in Railway**:
   - In your Railway project, click on your service ➔ **Variables** tab.
   - Add:
     - `TELEGRAM_BOT_TOKEN`: Your Telegram Bot token (from @BotFather)
     - `TELEGRAM_CHAT_ID`: Your Telegram Chat ID
     - `CUSTOM_RPCS`: *(Optional)* Comma-separated list of custom RPCs (e.g. Helius, Alchemy, QuickNode)
     - `HELIUS_API_KEY`: Your Helius API Key
     - `CONCURRENCY`: `4` (or `2` for low resource usage)
     - `BATCH_SIZE`: `100` (number of addresses per RPC request, max 100)
     - `DELAY_MS`: `200`
     - `RPC_TIMEOUT_SEC`: `5`

4. **Monitor Live Status**:
   - Under **Settings** -> **Networking**, generate a public domain (e.g. `your-app.up.railway.app`).
   - Open your browser or run `curl https://your-app.up.railway.app/` to view live stats:
     ```json
     {
       "status": "healthy",
       "uptime": "1h24m",
       "total_checked": 852000,
       "found": 0,
       "concurrency": 4,
       "batch_size": 100,
       "rpc_pool": [
         {
           "url": "https://mainnet.helius-rpc.com/?api-key=...",
           "is_healthy": true,
           "cooldown_left": "0s",
           "success_count": 5400,
           "fail_count": 0
         },
         {
           "url": "https://api.mainnet-beta.solana.com",
           "is_healthy": true,
           "cooldown_left": "0s",
           "success_count": 2125,
           "fail_count": 0
         }
       ]
     }
     ```

---

## ⚙️ Configuration Reference

| Variable | Default | Description |
| :--- | :--- | :--- |
| `HELIUS_API_KEY` | `dad6493c-...` | Dedicated Helius API Key for high-speed RPC |
| `TELEGRAM_BOT_TOKEN` | *None* | Bot token for Telegram alerts |
| `TELEGRAM_CHAT_ID` | *None* | Chat ID where alerts are delivered |
| `CUSTOM_RPCS` | *None* | Comma-separated list of additional RPC endpoints |
| `CONCURRENCY` | `4` | Number of parallel worker routines |
| `BATCH_SIZE` | `100` | Number of addresses queried per RPC request (up to 100) |
| `DELAY_MS` | `200` | Delay between batches in milliseconds |
| `RPC_TIMEOUT_SEC` | `5` | Timeout per RPC request in seconds |
| `VERBOSE_LOGS` | `false` | Enable per-address logging (set `false` on Railway to save bandwidth & CPU) |
| `PORT` | `8080` | Port for the HTTP health check server |
