# LOB Data Engine

A high-performance, production-ready system for collecting, processing, and managing Limit Order Book (LOB) and trade data from major cryptocurrency exchanges (Binance, Hyperliquid).

## Features

- **Real-time Data Collection**: Low-latency WebSocket listeners for Binance (Spot & Perp) and Hyperliquid.
- **Robustness**: Automatic reconnection, heartbeat monitoring, and VPS-based redundancy.
- **Data Persistence**: Efficient storage using Parquet with Snappy compression.
- **Historical Data**: Tools to download and process historical data from official sources.
- **Data Integrity**: Automated job system to fill local data gaps using remote VPS logs.
- **Processing Pipeline**: Labeling, sampling, and feature extraction for machine learning.

## Installation

Prerequisites: Python 3.9+

```bash
# Clone the repository
git clone <repository_url>
cd lob_data_engine

# Install dependencies
pip install -e .
```

## Quick Start
### 1. Real-time Collection
Start the listeners using the provided scripts:

```bash
# Start Binance Perpetual listener
python scripts/start_binance_perp.py

# Start all listeners
python scripts/start_listeners.py
```

### 2. Historical Data
Download historical data using the loader module:
```bash
# Example usage (adjust command based on implementation)
python -m lob_data_engine.historical_loader.main --symbol BTCUSDT --date 2024-01-01
```

### 3. Gap Filling (Data Repair)
If local data is missing (due to network issues), use the gap filler job to sync with your VPS:

```bash
python -m lob_data_engine.jobs.fill_gap /path/to/local/data --remote_alias vps
```

## Project Structure

```
lob_data_engine/
├── lob_data_engine/       # Main package
│   ├── binance_listener/  # Binance Spot listener
│   ├── binance_perp_listener/ # Binance Perp listener
│   ├── hyperliquid_listener/ # Hyperliquid listener
│   ├── historical_loader/ # Historical data downloader
│   ├── jobs/              # Data maintenance jobs (gap filling)
│   ├── process/           # Data processing & labeling
│   ├── manager.py         # WebSocket manager
│   └── schemas.py         # Data schemas
├── scripts/               # Entry point scripts
├── tests/                 # Unit tests
├── utils/                 # Utility scripts
└── pyproject.toml         # Project configuration
```

## Configuration

Configuration is managed via `config.py` files within modules or `vps_config.json` for remote settings. Ensure `vps_config.json` is properly set up for gap filling.

## License

MIT License
