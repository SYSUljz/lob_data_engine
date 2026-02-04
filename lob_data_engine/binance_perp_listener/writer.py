from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lob_data_engine.raw.schemas import BinancePerpTrade, BinanceDiff, BinanceSnapshot, BinanceData
from lob_data_engine.logging.factory import get_logger

logger = get_logger("writer", "BinancePerp")

class ParquetWriter:
    def __init__(self, output_dir: str, flush_interval_seconds: int = 60, batch_size: int = 10000):
        self.output_dir = output_dir
        self.flush_interval_seconds = flush_interval_seconds
        self.batch_size = batch_size
        self.queue: asyncio.Queue[BinanceData] = asyncio.Queue()
        self._running = False
        self._process_task: asyncio.Task | None = None

    async def start(self):
        self._running = True
        os.makedirs(self.output_dir, exist_ok=True)
        self._process_task = asyncio.create_task(self._process_queue())

    async def stop(self):
        self._running = False
        await self.queue.put(None)
        if self._process_task:
            await self._process_task

    async def add_data(self, data: BinanceData):
        await self.queue.put(data)

    async def _process_queue(self):
        buffer: List[BinanceData] = []
        last_flush_time = datetime.now().timestamp()

        while self._running or not self.queue.empty():
            try:
                # Calculate timeout for flush interval
                now = datetime.now().timestamp()
                time_since_flush = now - last_flush_time
                timeout = max(0.1, self.flush_interval_seconds - time_since_flush)

                try:
                    record = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                    if record is None:
                        continue
                    buffer.append(record)
                except asyncio.TimeoutError:
                    pass

                # Check if we need to flush
                now = datetime.now().timestamp()
                if len(buffer) >= self.batch_size or (now - last_flush_time >= self.flush_interval_seconds and buffer):
                    await self._flush_buffer(buffer)
                    buffer = []
                    last_flush_time = now
                
            except Exception as e:
                logger.error(f"Error in process queue: {e}")
                await asyncio.sleep(1)

        # Flush any remaining data in buffer after loop exits
        if buffer:
            await self._flush_buffer(buffer)

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
            
            # Check if we have the expected columns from Binance (s=symbol, e=event)
            # If not (e.g. unknown message), we might need a fallback.
            if "s" in df.columns and "e" in df.columns:
                # Group by symbol (s) and event type (e)
                groups = df.groupby(["s", "e"])

                for (symbol, event_type), group_df in groups:
                    # Use 'E' (event time) if available for naming and directory
                    if "E" in group_df.columns:
                        min_e = group_df["E"].min()
                        # Binance E is in milliseconds, using UTC for consistency
                        dt = datetime.utcfromtimestamp(min_e / 1000.0)
                        date_str = dt.strftime("%Y-%m-%d")
                        timestamp_ms = int(min_e)
                    else:
                        now = datetime.utcnow()
                        date_str = now.strftime("%Y-%m-%d")
                        timestamp_ms = int(now.timestamp() * 1000)

                    # Safe coin name for path
                    coin_clean = str(symbol).upper().replace("/", "_")
                    
                    # Construct path based on event type
                    # e.g. event_type = "depthUpdate" -> data/.../depthUpdate
                    # e.g. event_type = "trade" -> data/.../trade
                    target_dir = os.path.join(self.output_dir, date_str, coin_clean, str(event_type))
                    
                    os.makedirs(target_dir, exist_ok=True)
                    
                    filename = f"{event_type}_{timestamp_ms}.parquet"
                    file_path = os.path.join(target_dir, filename)
                    
                    # PyArrow handles nested lists (like 'b' and 'a') well usually.
                    table = pa.Table.from_pandas(group_df)
                    pq.write_table(table, file_path, compression='snappy')
                
                logger.info(f"Flushed {len(data)} records for keys: {list(groups.groups.keys())}")
            else:
                # Fallback if no symbol/event column
                now = datetime.utcnow()
                date_str = now.strftime("%Y-%m-%d")
                target_dir = os.path.join(self.output_dir, date_str, "unknown")
                os.makedirs(target_dir, exist_ok=True)
                filename = f"raw_{int(now.timestamp() * 1000)}.parquet"
                file_path = os.path.join(target_dir, filename)
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_path, compression='snappy')
                logger.info(f"Flushed {len(data)} records to {file_path} (no s/e columns)")
            
        except Exception as e:
            logger.error(f"Failed to write parquet: {e}")
            # In a real app, we might want to save failed buffer to a backup file or retry
