from __future__ import annotations

import asyncio
import json
import threading
import sys
import time
import subprocess
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

import requests

from lob_data_engine.raw.schemas import HyperliquidProcessedSnapshot, HyperliquidProcessedTrade
from lob_data_engine.logging.factory import get_logger

try:
    from . import config
    from .utils.writer import ParquetWriter
    from .websocket_manager import WebsocketManager
    from .utils.types import Subscription, WsMsg
except ImportError:
    import config
    from utils.writer import ParquetWriter
    from websocket_manager import WebsocketManager
    from utils.types import Subscription, WsMsg

# Logger configuration
logger = get_logger(name="main", exchange="Hyperliquid")

class HyperliquidListener:
    exchange: str = "Hyperliquid"

    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.writer: Optional[ParquetWriter] = None
        self.managers: List[WebsocketManager] = []

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.warning("HyperliquidListener is already running.")
            return

        self._thread = threading.Thread(target=self._run_thread, name="HyperliquidListenerThread", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._loop and self._stop_event:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        
        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("HyperliquidListener thread did not stop gracefully.")

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run_thread(self):
        try:
            asyncio.run(self._run_async())
        except Exception as e:
            logger.error(f"HyperliquidListener thread crashed: {e}")

    async def _run_async(self):
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        # 1. Identify Coins
        perp_coins = config.TARGET_COINS
        spot_map = self._fetch_spot_coins(config.TARGET_COINS)
        spot_coins = list(spot_map.values())
        
        logger.info(f"Perp Coins: {perp_coins}")
        logger.info(f"Spot Coins: {spot_coins}")
        
        all_subscriptions: List[Subscription] = []
        
        for coin in perp_coins:
            all_subscriptions.append({"type": "trades", "coin": coin})

            base_sub = {"type": "l2Book", "coin": coin}
            if coin in config.COIN_CONFIG:
                conf = config.COIN_CONFIG[coin]
                n_sig_figs = conf.get("nSigFigs")
                
                if isinstance(n_sig_figs, list):
                    for n in n_sig_figs:
                        sub = base_sub.copy()
                        sub.update(conf)
                        sub["nSigFigs"] = n
                        all_subscriptions.append(sub)
                else:
                    sub = base_sub.copy()
                    sub.update(conf)
                    all_subscriptions.append(sub)
            else:
                all_subscriptions.append(base_sub)

        if not all_subscriptions:
            logger.error("No subscriptions generated. Exiting.")
            return

        subs_by_sigfigs = {}
        for sub in all_subscriptions:
            n = sub.get("nSigFigs")
            if n not in subs_by_sigfigs:
                subs_by_sigfigs[n] = []
            subs_by_sigfigs[n].append(sub)

        # 2. Initialize Writer
        self.writer = ParquetWriter(
            output_dir=config.DATA_DIR,
            flush_interval_seconds=config.FLUSH_INTERVAL,
            batch_size=config.BATCH_SIZE
        )
        await self.writer.start()

        # 3. Message Handler Logic
        loop = asyncio.get_running_loop()

        def create_message_handler(writer: ParquetWriter, n_sig_figs_context: int | None):
            async def async_handler(msg: WsMsg):
                channel = msg.get("channel")
                if channel == "l2Book":
                    data = msg.get("data")
                    if not data: return
                    
                    coin = data.get("coin")
                    levels = data.get("levels")
                    timestamp = data.get("time")
                    
                    if not (coin and levels and timestamp): return

                    bids_data = levels[0]
                    asks_data = levels[1]
                    
                    record = {
                        "coin": coin,
                        "channel": "l2Book",
                        "nSigFigs": n_sig_figs_context,
                        "exchange_time": timestamp,
                        "local_time": time.time(),
                        "bids_px": [l["px"] for l in bids_data],
                        "bids_sz": [l["sz"] for l in bids_data],
                        "bids_n": [l["n"] for l in bids_data],
                        "asks_px": [l["px"] for l in asks_data],
                        "asks_sz": [l["sz"] for l in asks_data],
                        "asks_n": [l["n"] for l in asks_data],
                    }
                    await writer.add_data(record)

                elif channel == "trades":
                    data = msg.get("data")
                    if data:
                        for trade in data:
                            record = {
                                "coin": trade["coin"],
                                "channel": "trades",
                                "exchange_time": trade["time"],
                                "local_time": time.time(),
                                "side": trade["side"],
                                "px": trade["px"],
                                "sz": trade["sz"],
                                "hash": trade["hash"],
                                "tid": trade.get("tid"),
                                "users": json.dumps(trade.get("users", [])),
                            }
                            await writer.add_data(record)

            def thread_callback(msg: Any):
                asyncio.run_coroutine_threadsafe(async_handler(msg), loop)
                
            return thread_callback

        # 4. Initialize WebSocket Managers
        base_url = config.API_URL
        if base_url.endswith("/info"):
            base_url = base_url[:-5]

        self.managers = []
        for n_sig, group_subs in subs_by_sigfigs.items():
            logger.info(f"Initializing manager for nSigFigs={n_sig} with {len(group_subs)} subscriptions")
            
            ws_manager = WebsocketManager(base_url=base_url)
            callback = create_message_handler(self.writer, n_sig)
            
            ws_manager.start()
            self.managers.append(ws_manager)
            
            for sub in group_subs:
                ws_manager.subscribe(sub, callback)

        # 5. Monitor Task
        monitor_task = asyncio.create_task(self._monitor_date_rollover())
        
        try:
            await self._stop_event.wait()
        finally:
            logger.info("Shutting down HyperliquidListener...")
            monitor_task.cancel()
            for manager in self.managers:
                manager.stop()
            
            if self.writer:
                await self.writer.stop()
            logger.info("HyperliquidListener shutdown complete.")

    async def _monitor_date_rollover(self):
        logger.info("Starting date rollover monitor...")
        current_date = datetime.utcnow().strftime("%Y-%m-%d")
        
        while True:
            await asyncio.sleep(60)
            new_date = datetime.utcnow().strftime("%Y-%m-%d")
            
            if new_date != current_date:
                logger.info(f"Date rollover detected: {current_date} -> {new_date}")
                prev_date = current_date
                current_date = new_date
                
                logger.info(f"Triggering daily processing for {prev_date}...")
                try:
                    subprocess.Popen(
                        [sys.executable, "src/process/process_lob.py", "--date", prev_date, "--sync"],
                        cwd=os.getcwd(),
                    )
                except Exception as e:
                    logger.error(f"Failed to spawn processing script: {e}")

    def _fetch_spot_coins(self, target_base_names: List[str]) -> Dict[str, str]:
        try:
            logger.info("Fetching spot metadata...")
            resp = requests.post(config.API_URL, json={"type": "spotMeta"}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            universe = data.get("universe", [])
            found_coins = {}
            universe_names = {u["name"] for u in universe}
            
            for base in target_base_names:
                if base in universe_names:
                    found_coins[base] = base
                else:
                    logger.warning(f"Could not find exact spot match for {base}")
            
            return found_coins
        except Exception as e:
            logger.error(f"Failed to fetch spot metadata: {e}")
            return {}