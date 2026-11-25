# Fetchy

Real-time crypto price fetcher. Streams live trade data from Binance via WebSocket and exposes it over a REST API.

## How it works
   

   Binance WebSocket (wss)
│
▼
tokio::spawn (per symbol)
│
▼
Arc<RwLock> ← shared state
│
▼
Axum HTTP API (port 3000)




Each symbol runs in its own async task with automatic reconnection. All tasks write to the same shared `HashMap`, which the HTTP handlers read from.

## Stack

| Component | Crate |
|-----------|-------|
| Async runtime | `tokio` |
| WebSocket client | `tokio-tungstenite` |
| HTTP server | `axum` |
| Serialization | `serde` / `serde_json` |
| TLS | `rustls` (ring) |

## Getting started

### Prerequisites

- Rust 1.75+
- `curl` (for testing)

### Run

```bash
cargo run   

## Test


# Health check
curl http://127.0.0.1:3000/health

# Latest BTC price
curl http://127.0.0.1:3000/api/v1/prices/BTCUSDT

# Latest ETH price
curl http://127.0.0.1:3000/api/v1/prices/ETHUSDT   