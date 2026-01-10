import json
import time
import threading
from collections import defaultdict
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

import websocket

from .utils.types import Subscription, WsMsg, WsMsg as WsMsgType
from lob_data_engine.logging.factory import get_logger

logger = get_logger(name="websocket_manager", exchange="Hyperliquid")

ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])

def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "userFills":
        return f'userFills:{subscription["user"].lower()}'
    elif subscription["type"] == "candle":
        return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFundings":
        return f'userFundings:{subscription["user"].lower()}'
    elif subscription["type"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    elif subscription["type"] == "webData2":
        return f'webData2:{subscription["user"].lower()}'
    elif subscription["type"] == "bbo":
        return f'bbo:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetCtx":
        return f'activeAssetCtx:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetData":
        return f'activeAssetData:{subscription["coin"].lower()},{subscription["user"].lower()}'
    return "unknown"

def ws_msg_to_identifier(ws_msg: WsMsgType) -> Optional[str]:
    if ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ws_msg["channel"] == "orderUpdates":
        return "orderUpdates"
    elif ws_msg["channel"] == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "bbo":
        return f'bbo:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetCtx" or ws_msg["channel"] == "activeSpotAssetCtx":
        return f'activeAssetCtx:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetData":
        return f'activeAssetData:{ws_msg["data"]["coin"].lower()},{ws_msg["data"]["user"].lower()}'
    return None

class WebsocketManager(threading.Thread):
    def __init__(self, base_url):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.subscriptions: List[Tuple[Subscription, int]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)
        
        self.lock = threading.Lock()
        
        self.base_url = base_url
        self.ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ws = None 
        
        # --- Config ---
        self.idle_threshold = 1.0      
        self.wait_pong_timeout = 2.0   
        
        self.last_response_time = 0    
        self.last_data_time = 0        
        self.last_quiet_log_time = 0   
        
        self.ping_sent_time = 0      
        self.is_awaiting_pong = False 
        
        self.stop_event = threading.Event()
        self.ws_connected_event = threading.Event()
        
        self.initial_reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.current_reconnect_delay = self.initial_reconnect_delay

    def run(self):
        while not self.stop_event.is_set():
            logger.info(f"Connecting to websocket: {self.ws_url}...")
            self.ws = websocket.WebSocketApp(
                self.ws_url, 
                on_message=self.on_message, 
                on_open=self.on_open,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            self.ws_connected_event.set()
            now = time.time()
            self.last_response_time = now
            self.last_data_time = now
            self.is_awaiting_pong = False 
            
            ping_thread = threading.Thread(target=self._keep_alive_loop)
            ping_thread.daemon = True
            ping_thread.start()
            
            self.ws.run_forever()
            
            self.ws_ready = False
            self.ws_connected_event.clear()
            ping_thread.join(timeout=2.0)
            
            if not self.stop_event.is_set():
                logger.warning(f"Connection lost. Reconnecting in {self.current_reconnect_delay}s...")
                time.sleep(self.current_reconnect_delay)
                self.current_reconnect_delay = min(self.max_reconnect_delay, self.current_reconnect_delay * 2)

    def _keep_alive_loop(self):
        logger.debug("Aggressive Keep-alive thread started.")
        
        while self.ws_connected_event.is_set() and not self.stop_event.is_set():
            current_time = time.time()
            self.ws_connected_event.wait(0.1) 
            
            if not self.ws_connected_event.is_set():
                break

            if not self.ws_ready:
                continue

            time_since_last_msg = current_time - self.last_response_time
            time_since_last_data = current_time - self.last_data_time

            if self.is_awaiting_pong:
                time_since_ping = current_time - self.ping_sent_time
                if time_since_ping > self.wait_pong_timeout:
                    logger.error(f"Network Timeout! No response for {time_since_ping:.2f}s. Reconnecting...")
                    if self.ws: self.ws.close()
                    break
            else:
                if time_since_last_msg > self.idle_threshold:
                    logger.debug(f"Idle for {time_since_last_msg:.2f}s, sending Ping...")
                    self._send_ping_payload()
                    self.ping_sent_time = current_time
                    self.is_awaiting_pong = True

            if not self.is_awaiting_pong and time_since_last_data > 5.0:
                if current_time - self.last_quiet_log_time > 5.0:
                    self.last_quiet_log_time = current_time

    def on_message(self, _ws, message):
        self.last_response_time = time.time()
        
        if message == "Websocket connection established.":
            return
        
        try:
            ws_msg: WsMsgType = json.loads(message)
        except json.JSONDecodeError:
            return

        identifier = ws_msg_to_identifier(ws_msg)
        
        if identifier == "pong":
            logger.debug("Websocket received pong (link active).")
            self.is_awaiting_pong = False 
            return
            
        if identifier is None:
            return

        self.last_data_time = time.time()
        self.is_awaiting_pong = False 
        
        with self.lock:
            active_subscriptions = self.active_subscriptions[identifier]
            callbacks_to_run = list(active_subscriptions)

        for active_subscription in callbacks_to_run:
            try:
                active_subscription.callback(ws_msg)
            except Exception as e:
                logger.error(f"Callback error: {e}")

    def on_open(self, _ws):
        logger.debug("on_open: Connection established")
        self.ws_ready = True
        self.current_reconnect_delay = self.initial_reconnect_delay
        now = time.time()
        self.last_response_time = now
        self.last_data_time = now
        with self.lock:
            for subscription, _ in self.subscriptions:
                self._safe_send(json.dumps({"method": "subscribe", "subscription": subscription}))

    def on_error(self, _ws, error):
        logger.error(f"Websocket error: {error}")

    def on_close(self, _ws, code, msg):
        logger.info(f"Websocket closed. Code: {code}")

    def _safe_send(self, data):
        try:
            if self.ws and self.ws.sock:
                self.ws.send(data)
        except Exception as e:
            logger.error(f"Send error: {e}")
            
    def _send_ping_payload(self):
        self._safe_send(json.dumps({"method": "ping"}))

    def stop(self):
        self.stop_event.set()
        self.ws_connected_event.clear()
        if self.ws: self.ws.close()

    def subscribe(self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None) -> int:
        with self.lock:
            if subscription_id is None:
                self.subscription_id_counter += 1
                subscription_id = self.subscription_id_counter
            self.subscriptions.append((subscription, subscription_id))
            identifier = subscription_to_identifier(subscription)
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id))
            if self.ws_ready:
                self._safe_send(json.dumps({"method": "subscribe", "subscription": subscription}))
            return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        with self.lock:
            self.subscriptions = [s for s in self.subscriptions if s[1] != subscription_id]
            identifier = subscription_to_identifier(subscription)
            active_subscriptions = self.active_subscriptions[identifier]
            new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
            if len(new_active_subscriptions) == 0 and self.ws_ready:
                self._safe_send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
            self.active_subscriptions[identifier] = new_active_subscriptions
            return len(active_subscriptions) != len(new_active_subscriptions)
