from __future__ import annotations

import asyncio
import json
import threading
import sys
import time
from typing import Dict, List, Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed
import requests

from lob_data_engine.raw.schemas import BinancePerpTrade, BinanceDiff, BinanceSnapshot
from lob_data_engine.logging.factory import get_logger

try:
    from . import config
    from .writer import ParquetWriter
    from .orderbook import StreamSequenceVerifier, GapDetectedError
    from .websocket_manager import WebSocketManager
except ImportError:
    import config
    from writer import ParquetWriter
    from orderbook import StreamSequenceVerifier, GapDetectedError
    from websocket_manager import WebSocketManager

# Logger configuration
logger = get_logger("listener", "BinancePerp")

class BinancePerpListener:
    exchange: str = "BinancePerp"

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.writer: Optional[ParquetWriter] = None
        self.ws_manager: Optional[WebSocketManager] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.warning("BinancePerpListener is already running.")
            return

        self._thread = threading.Thread(target=self._run_thread, name="BinancePerpListenerThread", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._loop and self._stop_event:
            # Thread-safe way to set the asyncio event from another thread
            self._loop.call_soon_threadsafe(self._stop_event.set)
        
        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("BinancePerpListener thread did not stop gracefully.")

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _fetch_snapshot_sync(self, symbol: str) -> Optional[Dict[str, Any]]:
        try:
            # Default limit 1000 to get a deep book for snapshot
            # URL for Perpetual Futures
            url = "https://fapi.binance.com/fapi/v1/depth"
            params = {"symbol": symbol.upper(), "limit": 1000}
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Snapshot HTTP request failed for {symbol}: {e}")
            return None

    async def _fetch_and_save_snapshot(self, symbol: str):
        snapshot = await asyncio.to_thread(self._fetch_snapshot_sync, symbol)
        if snapshot:
            # Inject metadata
            timestamp_ms = int(time.time() * 1000)
            snapshot["e"] = "depthSnapshot"
            snapshot["E"] = timestamp_ms
            snapshot["s"] = symbol
            snapshot["local_time"] = time.time()
            snapshot["is_revalidation"] = False # Flag for downstream if needed
            
            if self.writer:
                await self.writer.add_data(snapshot)
                logger.info(f"[{symbol}] Snapshot saved. lastUpdateId={snapshot.get('lastUpdateId')}")

    def _run_thread(self):
        try:
            asyncio.run(self._run_async())
        except Exception as e:
            logger.error(f"BinancePerpListener thread crashed: {e}")

    async def _run_async(self):
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        # 1. Setup Configuration
        target_coins = config.TARGET_COINS
        
        verifiers: Dict[str, StreamSequenceVerifier] = {c: StreamSequenceVerifier(c) for c in target_coins}
        
        streams = []
        for coin in target_coins:
            c = coin.lower()
            if getattr(config, 'DEPTH_STREAM_TYPE', 'diff') == 'partial':
                levels = getattr(config, 'DEPTH_LEVELS', 20)
                streams.append(f"{c}@depth{levels}@{config.DEPTH_SPEED}")
            else:
                streams.append(f"{c}@depth@{config.DEPTH_SPEED}")
                
            streams.append(f"{c}@aggTrade")
            streams.append(f"{c}@forceOrder")

        logger.info(f"Target Coins: {target_coins}")
        logger.info(f"Streams to subscribe: {streams}")

        # 2. Initialize Writer
        self.writer = ParquetWriter(
            output_dir=config.DATA_DIR,
            flush_interval_seconds=config.FLUSH_INTERVAL,
            batch_size=config.BATCH_SIZE
        )
        await self.writer.start()

        # 3. Initialize WebSocketManager
        # URL for Perpetual Futures Combined Streams
        ws_url = "wss://fstream.binance.com/stream"
        
        # Calculate timeout based on DEPTH_SPEED (e.g. "100ms" -> 0.1s). Timeout = 2 * interval
        speed_str = str(config.DEPTH_SPEED)
        interval_sec = 0.1 # Default
        if speed_str.endswith("ms"):
            try:
                interval_sec = int(speed_str[:-2]) / 1000.0
            except ValueError:
                pass
        
        reconnect_timeout = interval_sec * 10.0
        
        self.ws_manager = WebSocketManager(ws_url, timeout=reconnect_timeout)
        self.ws_manager.subscribe(streams)

        # 4. Define Message Processing Callback
        async def process_message(msg: Dict[str, Any]):
            try:
                # Expecting combined stream format: {"stream": "...", "data": {...}}
                if "stream" in msg and "data" in msg:
                    data = msg["data"]
                    if isinstance(data, dict):
                        data['local_time'] = time.time()
                        
                        # Write data
                        if self.writer:
                            # Pre-processing for forceOrder to expose 's' (symbol) to writer
                            if data.get("e") == "forceOrder" and "o" in data:
                                order_data = data["o"]
                                if "s" in order_data:
                                    data["s"] = order_data["s"]

                            await self.writer.add_data(data)

                        # Sequence Verification
                        event_type = data.get("e")
                        symbol = data.get("s")

                        if event_type == "depthUpdate" and symbol in verifiers:
                            try:
                                verifiers[symbol].process_update(data)
                            except GapDetectedError as e:
                                logger.error(f"[{symbol}] Gap: {e}. Resetting sequence tracking and fetching snapshot...")
                                verifiers[symbol].last_update_id = None
                                # Trigger snapshot fetch to re-sync
                                asyncio.create_task(self._fetch_and_save_snapshot(symbol))
            except Exception as e:
                logger.error(f"Error in process_message: {e}")

        self.ws_manager.add_callback(process_message)

        # 5. Start Manager & Wait
        manager_task = asyncio.create_task(self.ws_manager.run())
        
        # Wait for WS connection to be established
        logger.info("Waiting for WebSocket connection...")
        while not self.ws_manager.is_connected and not self._stop_event.is_set():
            await asyncio.sleep(0.1)

        if not self._stop_event.is_set():
            logger.info("WebSocket connected. Waiting 2s for buffering...")
            await asyncio.sleep(2.0)
            
            # Fetch initial snapshots to ensure we can link diff stream (U <= lastUpdateId + 1 <= u)
            logger.info("Fetching initial snapshots...")
            for coin in target_coins:
                asyncio.create_task(self._fetch_and_save_snapshot(coin))
        
        try:
            # Wait until stopped
            await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Stopping BinancePerpListener...")
            # Shutdown sequence
            if self.ws_manager:
                await self.ws_manager.stop()
            
            # Wait for manager to finish
            try:
                await asyncio.wait_for(manager_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except Exception as e:
                logger.error(f"Error stopping manager task: {e}")

            if self.writer:
                await self.writer.stop()
            
            logger.info("BinancePerpListener stopped.")