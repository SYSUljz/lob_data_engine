from __future__ import annotations

import asyncio
import json
import logging
import threading
import sys
import time
from typing import Dict, List, Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed
import requests

from src.raw_data_schemas import BinanceTrade, BinanceDiff, BinanceSnapshot

try:
    from . import config
    from .writer import ParquetWriter
    from .orderbook import LocalOrderBook, GapDetectedError
except ImportError:
    import config
    from writer import ParquetWriter
    from orderbook import LocalOrderBook, GapDetectedError

# Logger configuration is now assumed to be handled by the Manager or globally,
# but we can keep a module-level logger.
logger = logging.getLogger("binance_listener")

async def fetch_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Fetches the REST snapshot for a given symbol.
    Runs in a separate thread to avoid blocking the asyncio loop.
    """
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"
    return await asyncio.to_thread(lambda: requests.get(url).json())

class BinanceListener:
    exchange: str = "Binance"

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.writer: Optional[ParquetWriter] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.warning("BinanceListener is already running.")
            return

        self._thread = threading.Thread(target=self._run_thread, name="BinanceListenerThread", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._loop and self._stop_event:
            # Thread-safe way to set the asyncio event from another thread
            self._loop.call_soon_threadsafe(self._stop_event.set)
        
        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("BinanceListener thread did not stop gracefully.")

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run_thread(self):
        try:
            asyncio.run(self._run_async())
        except Exception as e:
            logger.error(f"BinanceListener thread crashed: {e}")

    async def _run_async(self):
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        # 1. Setup Configuration
        target_coins = config.TARGET_COINS
        
        lobs: Dict[str, LocalOrderBook] = {c: LocalOrderBook(c) for c in target_coins}
        msg_buffers: Dict[str, List[Dict]] = {c: [] for c in target_coins}
        
        streams = []
        for coin in target_coins:
            c = coin.lower()
            if getattr(config, 'DEPTH_STREAM_TYPE', 'diff') == 'partial':
                levels = getattr(config, 'DEPTH_LEVELS', 20)
                streams.append(f"{c}@depth{levels}@{config.DEPTH_SPEED}")
            else:
                streams.append(f"{c}@depth@{config.DEPTH_SPEED}")
                
            streams.append(f"{c}@trade")

        logger.info(f"Target Coins: {target_coins}")
        logger.info(f"Streams to subscribe: {streams}")

        # 2. Initialize Writer
        self.writer = ParquetWriter(
            output_dir=config.DATA_DIR,
            flush_interval_seconds=config.FLUSH_INTERVAL,
            batch_size=config.BATCH_SIZE
        )
        await self.writer.start()

        ws_url = "wss://stream.binance.com:9443/stream"

        # --- Helper Functions ---
        async def perform_snapshot_sync(coin: str, is_revalidation: bool = False):
            try:
                if is_revalidation:
                    logger.info(f"[{coin}] Starting periodic validation. Pausing updates...")
                    lobs[coin].initialized = False
                
                # logger.info(f"[{coin}] Fetching REST snapshot...")
                snapshot_data = await fetch_snapshot(coin)
                
                if "lastUpdateId" not in snapshot_data:
                    logger.error(f"[{coin}] Invalid snapshot data: {snapshot_data}")
                    return

                if is_revalidation:
                    old_state = lobs[coin].get_best_prices()
                    new_bid = float(snapshot_data["bids"][0][0]) if snapshot_data["bids"] else None
                    new_ask = float(snapshot_data["asks"][0][0]) if snapshot_data["asks"] else None
                    new_id = snapshot_data["lastUpdateId"]
                    
                    logger.info(f"[{coin}] Consistency Check: Local(ID={old_state['lastUpdateId']}) Remote(ID={new_id})")

                snapshot_event = {
                    "e": "depthSnapshot",
                    "E": int(time.time() * 1000),
                    "s": coin,
                    "lastUpdateId": snapshot_data["lastUpdateId"],
                    "bids": snapshot_data.get("bids", []),
                    "asks": snapshot_data.get("asks", []),
                    "local_time": time.time(),
                    "is_revalidation": is_revalidation
                }
                await self.writer.add_data(snapshot_event)

                lobs[coin].set_snapshot(snapshot_data)

                buffer_len = len(msg_buffers[coin])
                applied_count = 0
                for buffered_msg in msg_buffers[coin]:
                    if lobs[coin].apply_update(buffered_msg):
                        applied_count += 1
                
                logger.info(f"[{coin}] Synced. Replayed {applied_count}/{buffer_len} buffered msgs.")
                msg_buffers[coin] = []

            except Exception as e:
                logger.error(f"[{coin}] Snapshot sync failed: {e}")

        async def manage_initial_snapshots():
            await asyncio.sleep(1)
            for coin in target_coins:
                if self._stop_event.is_set(): break
                await perform_snapshot_sync(coin, is_revalidation=False)

        async def periodic_validation_task():
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=config.VALIDATION_INTERVAL)
                    # If we woke up because stop_event is set, break
                    if self._stop_event.is_set(): break
                except asyncio.TimeoutError:
                    pass # Timeout means it's time to run validation

                if self._stop_event.is_set(): break
                
                logger.info("--- Starting Periodic Consistency Check ---")
                for coin in target_coins:
                    if self._stop_event.is_set(): break
                    await perform_snapshot_sync(coin, is_revalidation=True)
                logger.info("--- Consistency Check Complete ---")

        # --- Main Loop ---
        while not self._stop_event.is_set():
            try:
                logger.info(f"Connecting to {ws_url}...")
                async with websockets.connect(ws_url, ping_interval=None, max_queue=10000) as ws:
                    logger.info("Connected.")
                    
                    subscribe_msg = {
                        "method": "SUBSCRIBE",
                        "params": streams,
                        "id": 1
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info("Sent subscription request.")

                    snapshot_task = asyncio.create_task(manage_initial_snapshots())
                    validation_task = asyncio.create_task(periodic_validation_task())

                    try:
                        while not self._stop_event.is_set():
                            try:
                                msg_raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                                msg = json.loads(msg_raw)
                                
                                if "stream" in msg and "data" in msg:
                                    data = msg["data"]
                                    if isinstance(data, dict):
                                        data['local_time'] = time.time()
                                        await self.writer.add_data(data)

                                        event_type = data.get("e")
                                        symbol = data.get("s")

                                        if event_type == "depthUpdate" and symbol in lobs:
                                            if lobs[symbol].initialized:
                                                try:
                                                    lobs[symbol].apply_update(data)
                                                except GapDetectedError as e:
                                                    logger.error(f"[{symbol}] Gap: {e}. Recovering...")
                                                    lobs[symbol].initialized = False
                                                    asyncio.create_task(perform_snapshot_sync(symbol))
                                            else:
                                                msg_buffers[symbol].append(data)

                            except asyncio.TimeoutError:
                                continue
                            except ConnectionClosed:
                                logger.warning("Connection closed by server.")
                                break
                            except Exception as e:
                                logger.error(f"Error processing message: {e}")
                                continue
                    finally:
                        snapshot_task.cancel()
                        validation_task.cancel()
                        try: await snapshot_task 
                        except: pass
                        try: await validation_task
                        except: pass

            except Exception as e:
                logger.error(f"Connection error: {e}")
                if not self._stop_event.is_set():
                    await asyncio.sleep(5)

        if self.writer:
            await self.writer.stop()
        logger.info("BinanceListener stopped.")

