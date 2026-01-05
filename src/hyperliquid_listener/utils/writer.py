from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

class ParquetWriter:
    def __init__(self, output_dir: str, flush_interval_seconds: int = 60, batch_size: int = 10000):
        self.output_dir = output_dir
        self.flush_interval_seconds = flush_interval_seconds
        self.batch_size = batch_size
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._running = False
        self._process_task: asyncio.Task | None = None

    async def start(self):
        self._running = True
        os.makedirs(self.output_dir, exist_ok=True)
        self._process_task = asyncio.create_task(self._process_queue())

    async def stop(self):
        self._running = False
        if self._process_task:
            await self._process_task

    async def add_data(self, data: Dict[str, Any]):
        await self.queue.put(data)

    async def _process_queue(self):
        buffer: List[Dict[str, Any]] = []
        last_flush_time = datetime.now().timestamp()

        while self._running or not self.queue.empty():
            try:
                # Calculate timeout for flush interval
                now = datetime.now().timestamp()
                time_since_flush = now - last_flush_time
                timeout = max(0.1, self.flush_interval_seconds - time_since_flush)

                try:
                    record = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                    buffer.append(record)
                except asyncio.TimeoutError:
                    pass

                # Check if we need to flush
                now = datetime.now().timestamp()
                if len(buffer) >= self.batch_size or (now - last_flush_time >= self.flush_interval_seconds and buffer):
                    await self._flush_buffer(buffer)
                    buffer = []
                    last_flush_time = now
                
                # If we are stopping and queue is empty, flush remaining
                if not self._running and self.queue.empty() and buffer:
                    await self._flush_buffer(buffer)
                    buffer = []

            except Exception as e:
                logger.error(f"Error in process queue: {e}")
                await asyncio.sleep(1)

    async def _flush_buffer(self, buffer: List[Dict[str, Any]]):
        if not buffer:
            return
        # Offload to thread to avoid blocking event loop
        await asyncio.to_thread(self._write_to_disk, buffer)

    def _write_to_disk(self, data: List[Dict[str, Any]]):
        try:
            if not data:
                return

            df = pd.DataFrame(data)
            
            # Determine the timestamp to use for directory and filename
            # Prioritize 'exchange_time' from the data
            # If not present, fall back to current UTC time.
            if "exchange_time" in df.columns and not df["exchange_time"].empty:
                # Use the latest timestamp from the batch for consistency
                # Assuming exchange_time is in milliseconds
                timestamp_ms = df["exchange_time"].max()
            else:
                timestamp_ms = datetime.utcnow().timestamp() * 1000
            
            effective_datetime = datetime.utcfromtimestamp(timestamp_ms / 1000)
            date_str = effective_datetime.strftime("%Y-%m-%d")
            
            # Group by coin and nSigFigs to write separate files
            if "coin" in df.columns:
                # Ensure 'channel' and 'nSigFigs' columns exist
                if "channel" not in df.columns:
                    df["channel"] = "unknown"
                
                if "nSigFigs" not in df.columns:
                    df["nSigFigs"] = "default"
                else:
                    # Fill NaN nSigFigs
                    df["nSigFigs"] = df["nSigFigs"].fillna("default")

                groups = df.groupby(["coin", "channel", "nSigFigs"])

                for (coin, channel, n_sig_figs), group_df in groups:
                    # Safe coin name for path
                    coin_clean = str(coin).replace("/", "_")
                    
                    # Construct path based on channel
                    if channel == "l2Book":
                         n_sig_str = f"nSigFigs={n_sig_figs}"
                         target_dir = os.path.join(self.output_dir, date_str, coin_clean, n_sig_str)
                         filename_prefix = "l2book"
                    elif channel == "trades":
                         target_dir = os.path.join(self.output_dir, date_str, coin_clean, "trades")
                         filename_prefix = "trades"
                    else:
                         target_dir = os.path.join(self.output_dir, date_str, coin_clean, str(channel))
                         filename_prefix = str(channel)

                    os.makedirs(target_dir, exist_ok=True)
                    
                    filename = f"{filename_prefix}_{int(timestamp_ms)}.parquet"
                    file_path = os.path.join(target_dir, filename)
                    
                    table = pa.Table.from_pandas(group_df)
                    pq.write_table(table, file_path, compression='snappy')
                
                logger.info(f"Flushed {len(data)} records for keys: {list(groups.groups.keys())}")
            else:
                # Fallback if no coin column (shouldn't happen based on logic)
                target_dir = os.path.join(self.output_dir, date_str, "unknown")
                os.makedirs(target_dir, exist_ok=True)
                filename = f"l2book_{int(timestamp_ms)}.parquet"
                file_path = os.path.join(target_dir, filename)
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_path, compression='snappy')
                logger.info(f"Flushed {len(data)} records to {file_path} (no coin column)")
            
        except Exception as e:
            logger.error(f"Failed to write parquet: {e}")
            # In a real app, we might want to save failed buffer to a backup file or retry
