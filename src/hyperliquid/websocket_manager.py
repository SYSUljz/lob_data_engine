import json
import time
import logging
import threading
from collections import defaultdict

import websocket
from websocket import WebSocketConnectionClosedException

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])

# --- 保持原有的 identifier 转换函数不变 ---
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

def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
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

import json
import time
import logging
import threading
from collections import defaultdict

import websocket
# from websocket import WebSocketConnectionClosedException # 代码中未直接使用，保留或注释

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])

# ... (subscription_to_identifier 和 ws_msg_to_identifier 函数保持不变，此处省略) ...
# 请保留你原有的 subscription_to_identifier 和 ws_msg_to_identifier 函数代码

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
        
        # --- 修改配置：激进的心跳参数 ---
        # 1. 如果超过 idle_threshold 秒没有收到任何消息，立即发送 Ping
        self.idle_threshold = 1.0  
        # 2. 发送 Ping 后，如果 wait_pong_timeout 秒内没有收到任何消息（Pong或数据），视为断连
        self.wait_pong_timeout = 2.0 
        
        self.last_response_time = 0 
        self.ping_sent_time = 0      # 记录发送 Ping 的时间
        self.is_awaiting_pong = False # 标记是否正在等待 Pong 回复
        
        self.stop_event = threading.Event()
        self.ws_connected_event = threading.Event()
        
        self.initial_reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.current_reconnect_delay = self.initial_reconnect_delay

    def run(self):
        while not self.stop_event.is_set():
            logging.info(f"Connecting to websocket: {self.ws_url}...")
            
            self.ws = websocket.WebSocketApp(
                self.ws_url, 
                on_message=self.on_message, 
                on_open=self.on_open,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            self.ws_connected_event.set()
            self.last_response_time = time.time()
            # 重置心跳状态
            self.is_awaiting_pong = False 
            
            ping_thread = threading.Thread(target=self._keep_alive_loop)
            ping_thread.daemon = True
            ping_thread.start()
            
            self.ws.run_forever()
            
            # 清理阶段
            self.ws_ready = False
            self.ws_connected_event.clear()
            
            # 停止心跳线程
            ping_thread.join(timeout=2.0)
            
            if not self.stop_event.is_set():
                with self.lock:
                    subs = [s[0] for s in self.subscriptions]
                logging.warning(f"Connection lost. Reconnecting in {self.current_reconnect_delay}s...")
                time.sleep(self.current_reconnect_delay)
                self.current_reconnect_delay = min(self.max_reconnect_delay, self.current_reconnect_delay * 2)

    def _keep_alive_loop(self):
        """
        高频检测线程
        """
        logging.debug("Aggressive Keep-alive thread started.")
        
        while self.ws_connected_event.is_set() and not self.stop_event.is_set():
            current_time = time.time()
            
            # 既然要求1s检测，sleep时间必须很短，这里设为0.1s
            self.ws_connected_event.wait(0.1)
            
            # 如果连接已经断了，或者 stop 被设置，退出
            if not self.ws_connected_event.is_set():
                break

            # 1. 计算距离上一次收到消息（任何数据或pong）过去了多久
            time_since_last_msg = current_time - self.last_response_time
            
            # 状态 A: 正在等待 Pong (说明之前已经判定超时并发送了 Ping)
            if self.is_awaiting_pong:
                time_since_ping = current_time - self.ping_sent_time
                # 如果等待时间超过了容忍度，且依然没有新消息更新 last_response_time
                if time_since_ping > self.wait_pong_timeout:
                    # 再次确认一下 (防止在极短时间内收到了消息)
                    if time.time() - self.last_response_time > (self.idle_threshold + self.wait_pong_timeout):
                        logging.error(f"Timeout! Ping sent {time_since_ping:.2f}s ago, no response. Resetting.")
                        if self.ws:
                            self.ws.close()
                        break
            
            # 状态 B: 正常状态，检查是否空闲过久
            else:
                if time_since_last_msg > self.idle_threshold:
                    # 超过1秒没声音，马上 Ping
                    logging.debug(f"No data for {time_since_last_msg:.2f}s, sending proactive Ping...")
                    self._send_ping_payload()
                    self.ping_sent_time = time.time()
                    self.is_awaiting_pong = True # 进入“怀疑断连”状态

    def on_message(self, _ws, message):
        # --- 核心修改：收到任何消息都代表连接存活 ---
        self.last_response_time = time.time()
        
        # 如果之前在怀疑断连（等待Pong），现在收到了数据（无论是Pong还是Trade数据），说明连接是活的
        if self.is_awaiting_pong:
            self.is_awaiting_pong = False
            # logging.debug("Connection confirmed alive by incoming message.")

        if message == "Websocket connection established.":
            logging.debug(message)
            return
        
        try:
            ws_msg: WsMsg = json.loads(message)
        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON: {message}")
            return

        # 你的 ws_msg_to_identifier 需要能引入进来
        identifier = ws_msg_to_identifier(ws_msg)
        
        if identifier == "pong":
            logging.debug(f"Websocket received pong.")
            return
            
        if identifier is None:
            return
        
        with self.lock:
            active_subscriptions = self.active_subscriptions[identifier]
            callbacks_to_run = list(active_subscriptions)

        if len(callbacks_to_run) > 0:
            for active_subscription in callbacks_to_run:
                try:
                    active_subscription.callback(ws_msg)
                except Exception as e:
                    logging.error(f"Error in callback: {e}", exc_info=True)

    # ... (on_open, on_error, on_close, _safe_send, subscribe, unsubscribe 保持不变) ...
    # 下面补全省略的函数以确保类完整性
    
    def on_open(self, _ws):
        logging.debug("on_open: Connection established")
        self.ws_ready = True
        self.current_reconnect_delay = self.initial_reconnect_delay
        self.last_response_time = time.time()
        self.is_awaiting_pong = False
        
        with self.lock:
            for subscription, _ in self.subscriptions:
                self._safe_send(json.dumps({"method": "subscribe", "subscription": subscription}))

    def on_error(self, _ws, error):
        logging.error(f"Websocket error: {error}")

    def on_close(self, _ws, close_status_code, close_msg):
        logging.info(f"Websocket closed. Code: {close_status_code}")

    def _safe_send(self, data):
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.ws.send(data)
            else:
                logging.warning("Attempted to send message while socket is closed.")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            
    def _send_ping_payload(self):
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.ws.send(json.dumps({"method": "ping"}))
            else:
                logging.debug("Skipping ping: Socket not connected")
        except Exception as e:
            logging.warning(f"Failed to send ping: {e}")

    def stop(self):
        self.stop_event.set()
        self.ws_connected_event.clear()
        if self.ws:
            self.ws.close()

    def subscribe(self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None) -> int:
        # 与原代码一致
        with self.lock:
            if subscription_id is None:
                self.subscription_id_counter += 1
                subscription_id = self.subscription_id_counter
            self.subscriptions.append((subscription, subscription_id))
            identifier = subscription_to_identifier(subscription)
            if identifier == "userEvents" or identifier == "orderUpdates":
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id))
            if self.ws_ready:
                self._safe_send(json.dumps({"method": "subscribe", "subscription": subscription}))
            return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        # 与原代码一致
        with self.lock:
            self.subscriptions = [s for s in self.subscriptions if s[1] != subscription_id]
            identifier = subscription_to_identifier(subscription)
            active_subscriptions = self.active_subscriptions[identifier]
            new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
            if len(new_active_subscriptions) == 0 and self.ws_ready:
                self._safe_send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
            self.active_subscriptions[identifier] = new_active_subscriptions
            return len(active_subscriptions) != len(new_active_subscriptions)