import os
import io
import requests
import zipfile
import pandas as pd
import argparse
from datetime import datetime, timedelta
from typing import Optional, Literal
from pathlib import Path

# Base URL for Binance Vision Data
BINANCE_VISION_BASE_URL = "https://data.binance.vision"

class BinanceVisionDownloader:
    """
    Downloader for Binance Vision historical data.
    Specializes in Perpetual AggTrades.
    """

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _construct_url(
        self,
        symbol: str,
        date: str,
        market_type: Literal["cm", "um"] = "cm",
        data_type: str = "aggTrades"
    ) -> str:
        """
        Constructs the download URL for Binance Vision.
        Example: https://data.binance.vision/data/futures/cm/daily/aggTrades/BTCUSD_PERP/BTCUSD_PERP-aggTrades-2026-01-19.zip
        """
        # Ensure date is in YYYY-MM-DD format
        return f"{BINANCE_VISION_BASE_URL}/data/futures/{market_type}/daily/{data_type}/{symbol}/{symbol}-{data_type}-{date}.zip"

    def download_daily_agg_trades(
        self,
        symbol: str,
        date: str,
        market_type: Literal["cm", "um"] = "cm",
        save_parquet: bool = True
    ) -> Optional[Path]:
        """
        Downloads daily aggTrades, parses CSV, and optionally saves as Parquet.
        Returns the path to the saved file.
        """
        url = self._construct_url(symbol, date, market_type, "aggTrades")
        print(f"Downloading from: {url}")

        try:
            response = requests.get(url, stream=True)
            if response.status_code == 404:
                print(f"Data not found for {symbol} on {date} (404).")
                return None
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # We expect a single CSV file inside the zip with the same name scheme
                # usually {symbol}-aggTrades-{date}.csv
                csv_filename = f"{symbol}-aggTrades-{date}.csv"
                if csv_filename not in z.namelist():
                    # Fallback: find the first csv file
                    csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                    if not csv_files:
                        print("No CSV file found in the zip archive.")
                        return None
                    csv_filename = csv_files[0]
                
                print(f"Extracting and reading {csv_filename}...")
                with z.open(csv_filename) as f:
                    # Check first line to see if header exists
                    first_line = f.readline().decode('utf-8')
                    f.seek(0)
                    
                    has_header = "agg_trade_id" in first_line
                    header_arg = 0 if has_header else None

                    # Read CSV using Pandas
                    # Columns from schema: agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
                    df = pd.read_csv(
                        f,
                        header=header_arg,
                        names=[
                            "agg_trade_id", 
                            "price", 
                            "quantity", 
                            "first_trade_id", 
                            "last_trade_id", 
                            "transact_time", 
                            "is_buyer_maker"
                        ],
                        dtype={
                            "agg_trade_id": "int64",
                            "price": "str",      # Keep as string to match schema/avoid precision loss
                            "quantity": "str",   # Keep as string
                            "first_trade_id": "int64",
                            "last_trade_id": "int64",
                            "transact_time": "int64",
                            "is_buyer_maker": "bool"
                        }
                    )

            if save_parquet:
                output_filename = f"{symbol}-aggTrades-{date}.parquet"
                output_path = self.output_dir / output_filename
                
                # Use pyarrow engine for better performance and type support
                df.to_parquet(output_path, engine="pyarrow", index=False)
                print(f"Saved to {output_path}")
                return output_path
            
            return None

        except Exception as e:
            print(f"Error downloading or processing {symbol} on {date}: {e}")
            return None

def main():
    parser = argparse.ArgumentParser(description="Binance Vision AggTrades Downloader")
    parser.add_argument("--symbol", type=str, required=True, help="Trading symbol (e.g., BTCUSD_PERP)")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--output_dir", type=str, default="./data", help="Directory to save output Parquet files")
    parser.add_argument("--market_type", type=str, choices=["cm", "um"], default="cm", help="Market type: 'cm' (COIN-M) or 'um' (USD-M). Default: cm")
    
    args = parser.parse_args()
    
    downloader = BinanceVisionDownloader(output_dir=args.output_dir)
    downloader.download_daily_agg_trades(
        symbol=args.symbol, 
        date=args.date, 
        market_type=args.market_type
    )

if __name__ == "__main__":
    main()
