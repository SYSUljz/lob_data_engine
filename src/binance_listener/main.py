from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict, List, Any

import websockets
from websockets.exceptions import ConnectionClosed
import requests

try:
    import config
    from writer import ParquetWriter
    from orderbook import LocalOrderBook, GapDetectedError
except ImportError:
    # Attempt relative import if running as package
    from . import config
    from .writer import ParquetWriter
    from .orderbook import LocalOrderBook, GapDetectedError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("binance_listener")

async def fetch_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Fetches the REST snapshot for a given symbol.
    Runs in a separate thread to avoid blocking the asyncio loop.
    """
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"
    return await asyncio.to_thread(lambda: requests.get(url).json())

async def main():
    # 1. Setup Configuration
    target_coins = config.TARGET_COINS # e.g. ["BTCUSDT", ...]
    
    # Initialize Local Order Books and Buffers
    # We maintain a LOB for each coin to "apply depthUpdate" as requested.
    lobs: Dict[str, LocalOrderBook] = {c: LocalOrderBook(c) for c in target_coins}
    msg_buffers: Dict[str, List[Dict]] = {c: [] for c in target_coins}
    
    # Streams: <symbol>@depth20@100ms, <symbol>@trade
    streams = []
    for coin in target_coins:
        c = coin.lower()
        # Determine stream name based on config
        if getattr(config, 'DEPTH_STREAM_TYPE', 'diff') == 'partial':
            # Partial Depth Stream: <symbol>@depth<levels>@100ms
            levels = getattr(config, 'DEPTH_LEVELS', 20)
            streams.append(f"{c}@depth{levels}@{config.DEPTH_SPEED}")
        else:
            # Diff Depth Stream: <symbol>@depth@100ms
            streams.append(f"{c}@depth@{config.DEPTH_SPEED}")
            
        streams.append(f"{c}@trade")

    logger.info(f"Target Coins: {target_coins}")
    logger.info(f"Streams to subscribe: {streams}")

    # 2. Initialize Writer
    writer = ParquetWriter(
        output_dir=config.DATA_DIR,
        flush_interval_seconds=config.FLUSH_INTERVAL,
        batch_size=config.BATCH_SIZE
    )
    await writer.start()

    # 3. Connection Loop
    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info("Signal received, stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    ws_url = "wss://stream.binance.com:9443/stream"

    # --- Helper Functions (Moved Inside Main Scope) ---
    async def perform_snapshot_sync(coin: str, is_revalidation: bool = False):
        """
        Fetches snapshot, validates/resets LocalOrderBook, and replays buffer.
        Used for both initial startup and periodic consistency checks.
        """
        try:
            if is_revalidation:
                logger.info(f"[{coin}] Starting periodic validation. Pausing updates...")
                # Disable application of new updates to force buffering
                lobs[coin].initialized = False
            
            logger.info(f"[{coin}] Fetching REST snapshot...")
            snapshot_data = await fetch_snapshot(coin)
            
            if "lastUpdateId" not in snapshot_data:
                logger.error(f"[{coin}] Invalid snapshot data: {snapshot_data}")
                return

            # If revalidating, compare state before resetting
            if is_revalidation:
                old_state = lobs[coin].get_best_prices()
                new_bid = float(snapshot_data["bids"][0][0]) if snapshot_data["bids"] else None
                new_ask = float(snapshot_data["asks"][0][0]) if snapshot_data["asks"] else None
                new_id = snapshot_data["lastUpdateId"]
                
                logger.info(f"[{coin}] Consistency Check:\n" 
                            f"    Local (ID={old_state['lastUpdateId']}): Bid={old_state['best_bid']}, Ask={old_state['best_ask']}\n" 
                            f"    Remote(ID={new_id}): Bid={new_bid}, Ask={new_ask}")

            # 1. Save Snapshot to Writer
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
            await writer.add_data(snapshot_event)

            # 2. Initialize/Reset Local Order Book
            lobs[coin].set_snapshot(snapshot_data)

            # 3. Replay Buffered Messages
            buffer_len = len(msg_buffers[coin])
            applied_count = 0
            for buffered_msg in msg_buffers[coin]:
                if lobs[coin].apply_update(buffered_msg):
                    applied_count += 1
            
            logger.info(f"[{coin}] Synced. Replayed {applied_count}/{buffer_len} buffered msgs.")
            
            # Clear buffer
            msg_buffers[coin] = []

        except Exception as e:
            logger.error(f"[{coin}] Snapshot sync failed: {e}")

    async def manage_initial_snapshots():
        # Give a small pause to ensure subscription is active
        await asyncio.sleep(1)
        for coin in target_coins:
            if stop_event.is_set(): break
            await perform_snapshot_sync(coin, is_revalidation=False)

    async def periodic_validation_task():
        """Periodically re-fetches snapshots to ensure consistency."""
        while not stop_event.is_set():
            try:
                await asyncio.sleep(config.VALIDATION_INTERVAL)
                if stop_event.is_set(): break
                
                logger.info("--- Starting Periodic Consistency Check ---")
                for coin in target_coins:
                    if stop_event.is_set(): break
                    # Run sync for each coin
                    await perform_snapshot_sync(coin, is_revalidation=True)
                logger.info("--- Consistency Check Complete ---")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in validation task: {e}")

    # --- Main Loop ---
    while not stop_event.is_set():
        try:
            logger.info(f"Connecting to {ws_url}...")
            # Disable client-side pings to rely on Server's Pings (3min interval)
            # Increase max_queue to buffer more messages during snapshot processing
            async with websockets.connect(ws_url, ping_interval=None, max_queue=10000) as ws:
                logger.info("Connected.")
                
                # Subscribe
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": 1
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("Sent subscription request.")

                # Start the tasks
                snapshot_task = asyncio.create_task(manage_initial_snapshots())
                validation_task = asyncio.create_task(periodic_validation_task())

                # Message Loop
                try:
                    while not stop_event.is_set():
                        try:
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            msg = json.loads(msg_raw)
                            
                            # Handle Combined Stream Message
                            # {"stream": "...", "data": ...}
                            if "stream" in msg and "data" in msg:
                                data = msg["data"]
                                
                                # Ensure we have a valid dictionary to write
                                if isinstance(data, dict):
                                    data['local_time'] = time.time()
                                    await writer.add_data(data)

                                    # Logic to Apply depthUpdate to Snapshot
                                    event_type = data.get("e")
                                    symbol = data.get("s")

                                    if event_type == "depthUpdate" and symbol in lobs:
                                        if lobs[symbol].initialized:
                                            try:
                                                # Directly apply
                                                lobs[symbol].apply_update(data)
                                            except GapDetectedError as e:
                                                logger.error(f"[{symbol}] {e}. Entering recovery mode...")
                                                # Set initialized to False to start buffering new messages
                                                lobs[symbol].initialized = False
                                                # Trigger snapshot sync (recovery) in background
                                                asyncio.create_task(perform_snapshot_sync(symbol))
                                        else:
                                            # Buffer until snapshot is ready
                                            msg_buffers[symbol].append(data)

                            elif "id" in msg and msg.get("result") is None:
                                logger.info(f"Received system message: {msg}")

                        except asyncio.TimeoutError:
                            continue
                        except ConnectionClosed:
                            logger.warning("Connection closed by server.")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            continue
                finally:
                    # Clean up tasks if connection is lost
                    snapshot_task.cancel()
                    validation_task.cancel()
                    try:
                        await snapshot_task
                    except asyncio.CancelledError:
                        pass
                    try:
                        await validation_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logger.error(f"Connection error: {e}")
            if not stop_event.is_set():
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    await writer.stop()
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
