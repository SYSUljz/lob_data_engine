from __future__ import annotations

import asyncio
import json
import time
from typing import List, Callable, Optional, Dict, Any, Union

import websockets
from websockets.exceptions import ConnectionClosed

from lob_data_engine.logging.factory import get_logger

logger = get_logger("ws_manager", "Binance")

class WebSocketManager:
    """
    Manages WebSocket connection to Binance, handling:
    - Automatic connection and reconnection.
    - Subscription to streams.
    - Ping/Pong handling (Standard WebSocket Pings are handled automatically by the library).
    - Message routing to callbacks.
    """
    def __init__(self, url: str, timeout: float = 185.0):
        self.url = url
        self.timeout = timeout
        self.callbacks: List[Callable[[Dict[str, Any]], Any]] = []
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._subscriptions: List[str] = []
        self._reconnect_delay = 1
        self._lock = asyncio.Lock()

    def add_callback(self, callback: Callable[[Dict[str, Any]], Any]):
        """Register a callback to process received messages."""
        self.callbacks.append(callback)

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.state == websockets.protocol.State.OPEN

    def subscribe(self, streams: List[str]):
        """Queue subscriptions to be sent upon connection."""
        # Using a list to maintain order, but avoiding duplicates
        for s in streams:
            if s not in self._subscriptions:
                self._subscriptions.append(s)
        
        # If already connected, send immediately
        if self._ws and self._ws.state == websockets.protocol.State.OPEN:
            asyncio.create_task(self._send_subscribe(streams))

    async def _send_subscribe(self, streams: List[str]):
        if not streams or not self._ws:
            return
        try:
            msg = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": int(time.time() * 1000)
            }
            await self._ws.send(json.dumps(msg))
            logger.info(f"Sent subscription for {len(streams)} streams.")
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    async def run(self):
        self._running = True
        self._reconnect_delay = 1

        while self._running:
            try:
                logger.info(f"Connecting to {self.url}...")
                
                # ping_interval=None: We disable client-side heartbeats because Binance sends Pings.
                # The websockets library automatically responds to Pings with Pongs containing the same payload.
                # This satisfies the requirement: "When you receive a ping, you must send a pong with a copy of ping's payload"
                async with websockets.connect(
                    self.url, 
                    ping_interval=None, 
                    max_queue=10000,
                    ping_timeout=None, # We handle timeout manually via logic if needed, or rely on TCP
                    close_timeout=5
                ) as ws:
                    self._ws = ws
                    logger.info("Connected to Binance WebSocket.")
                    self._reconnect_delay = 1 # Reset delay on successful connect
                    
                    # Send subscriptions
                    if self._subscriptions:
                        await self._send_subscribe(self._subscriptions)

                    last_msg_time = time.time()

                    while self._running:
                        try:
                            # Wait for message with a timeout to allow checking self._running
                            # and to detect connection silence if needed.
                            # Binance sends pings every 3 mins.
                            # We set timeout to 60s. If no data for 60s, it's not necessarily an error 
                            # (if no market activity), but for major pairs, it usually is.
                            # However, `recv()` does NOT return Pings. So silence > 3 mins is suspicious.
                            
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
                            last_msg_time = time.time()
                            
                            msg = json.loads(msg_raw)
                            
                            # Defensive: Handle application-level "ping" JSON message if it were to occur
                            if isinstance(msg, dict) and msg.get("ping"):
                                pong_payload = msg.get("ping")
                                logger.debug(f"Received application ping: {pong_payload}, sending pong.")
                                await ws.send(json.dumps({"pong": pong_payload}))
                                continue

                            # Process Message
                            for callback in self.callbacks:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(msg)
                                else:
                                    callback(msg)

                        except asyncio.TimeoutError:
                            logger.warning(f"No data received for > {self.timeout}s. Reconnecting...")
                            break
                        
                        except ConnectionClosed:
                            logger.warning("Connection closed by server.")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            # Don't break loop for message processing errors
                            continue
            
            except Exception as e:
                logger.error(f"Connection error: {e}")
            
            if self._running:
                logger.info(f"Reconnecting in {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(60, self._reconnect_delay * 2)

        logger.info("WebSocketManager stopped.")
