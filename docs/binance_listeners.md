# Binance Data Listeners Documentation

This module provides listeners for capturing real-time order book and trade data from Binance (Spot) and Binance Perpetual Futures.

## Quick Start

The easiest way to manage the listeners is via the project's `makefile`.

### Starting Listeners

To start the **Binance Spot** listener in the background:
```bash
make start-binance
```

To start the **Binance Perpetual Futures** listener in the background:
```bash
make start-binance-perp
```

To start **both** listeners simultaneously:
```bash
make start-all
```

### Stopping Listeners

To stop the **Binance Spot** listener:
```bash
make stop-binance
```

To stop the **Binance Perpetual Futures** listener:
```bash
make stop-binance-perp
```

To stop **both** listeners:
```bash
make stop-all
```

### Checking Status

To check if the listeners are currently running:
```bash
make status
```

---

## Configuration

Configuration files are located in:
- Spot: `src/lob_data_engine/binance_listener/config.py`
- Perpetual: `src/lob_data_engine/binance_perp_listener/config.py`

### Key Settings

| Setting | Description | Default (Spot) | Default (Perp) |
| :--- | :--- | :--- | :--- |
| `TARGET_COINS` | List of trading pairs to monitor. | `["BTCUSDT", "ETHUSDT", "SOLUSDT"]` | `["BTCUSDT", "ETHUSDT", "SOLUSDT"]` |
| `DATA_DIR` | Output directory for captured data. | `./data` | `./perp_data` |
| `DEPTH_SPEED` | Update frequency for depth data. | `100ms` | `100ms` |
| `DEPTH_STREAM_TYPE` | Type of depth stream (`diff` or `partial`). | `partial` | `partial` |
| `DEPTH_LEVELS` | Number of levels for `partial` depth stream. | `20` | `20` (implicit) |
| `ENABLE_TRADE` | Capture raw individual trades. | `False` | `False` |
| `ENABLE_AGGTRADE` | Capture aggregate trades. | `False` | `False` |
| `ENABLE_FORCE_ORDER` | Capture liquidation orders (Perp only). | N/A | `True` |

### Trade Data Note

**Note:** `ENABLE_TRADE` and `ENABLE_AGGTRADE` are currently set to `False` by default.

**Reason:** We are not currently capturing real-time trade data because high-quality historical trade data is readily available directly from the Binance website. We prioritize capturing order book (depth) data which is harder to reconstruct historically with high precision.
