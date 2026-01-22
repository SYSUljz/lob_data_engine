# Data Gap Filling Jobs

This directory contains jobs for filling data gaps in local parquet files by fetching missing data from a remote VPS via `rclone`.

## Configuration

To avoid passing remote parameters every time, create a `vps_config.json` file in the project root directory.

### `vps_config.json` Template

```json
{
    "remote_alias": "vps",
    "remote_base_path": "/root/lob_data_engine/data"
}
```

- **remote_alias**: The alias name configured in `rclone` (e.g., `vps`).
- **remote_base_path**: The absolute path to the data directory on the remote server.

## Scripts

### 1. `fill_gap.py` (Order Book Depth)

Merges local depth update files (`depthUpdate/*.parquet`), detects sequence gaps based on update IDs (`u` and `U`), and downloads missing chunks from the remote server.

**Usage:**

```bash
python src/jobs/fill_gap.py <path_to_symbol_data> [options]
```

**Example:**

```bash
python src/jobs/fill_gap.py data/2026-01-15/BTCUSDT
```

**Options:**
- `--remote_alias`: Override the config value for the rclone alias.
- `--remote_base`: Override the config value for the remote base path.

### 2. `fill_trade_gap.py` (Trade Data)

Merges local trade files (`trade/*.parquet`), detects gaps in trade IDs (`t`), and downloads missing chunks.

**Usage:**

```bash
python src/jobs/fill_trade_gap.py <path_to_symbol_data> [options]
```

**Example:**

```bash
python src/jobs/fill_trade_gap.py data/2026-01-15/BTCUSDT
```

## How It Works

1. **Merge**: Consolidates all local `.parquet` files in the respective subfolder (e.g., `depthUpdate` or `trade`) into a single `merged.parquet`.
2. **Analyze**: Scans the merged data for discontinuities in sequence IDs (`u` for depth, `t` for trades).
3. **Fetch**: Connects to the remote VPS using `rclone` and lists available files.
4. **Match**: Identifies remote files that cover the detected time gaps.
5. **Patch**: Downloads the necessary files, merges them with the local data, and saves the result as `filled.parquet`.
6. **Report**: Generates a text report in `filled/gap_report.txt` detailing the operation.
