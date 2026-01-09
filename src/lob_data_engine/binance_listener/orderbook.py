from typing import Dict, List, Any, Optional
import logging

try:
    from messages import DepthUpdateMsg
except ImportError:
    from .messages import DepthUpdateMsg

logger = logging.getLogger("binance_listener.orderbook")

class GapDetectedError(Exception):
    """Raised when a sequence gap is detected in the order book stream."""
    pass

class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id = 0
        self.initialized = False
        self.synced = False

    def set_snapshot(self, snapshot_data: Dict[str, Any]):
        """
        Initialize the order book with a REST snapshot.
        snapshot_data: The JSON response from GET /api/v3/depth
        """
        self.last_update_id = snapshot_data["lastUpdateId"]
        
        # Clear existing
        self.bids.clear()
        self.asks.clear()

        # Populate (Price, Qty)
        for price_str, qty_str in snapshot_data.get("bids", []):
            self.bids[float(price_str)] = float(qty_str)
            
        for price_str, qty_str in snapshot_data.get("asks", []):
            self.asks[float(price_str)] = float(qty_str)
            
        self.initialized = True
        self.synced = False
        logger.info(f"[{self.symbol}] Order book initialized. LastUpdateId: {self.last_update_id}")

    def apply_update(self, update_data: DepthUpdateMsg) -> bool:
        """
        Apply a depthUpdate event.
        Returns True if applied, False if ignored (stale).
        Raises GapDetectedError if a sequence gap is detected.
        """
        if not self.initialized:
            # Buffer or ignore if handled externally
            return False

        u = update_data["u"] # Final update ID
        U = update_data["U"] # First update ID
        # pu = update_data.get("pu") # Previous update ID

        # 1. Drop any event where u is <= lastUpdateId
        if u <= self.last_update_id:
            return False

        # 2. Sequence Check
        if not self.synced:
            # Strict recovery: Must find diff where U = lastUpdateId + 1
            if U == self.last_update_id + 1:
                self.synced = True
            else:
                # Any misalignment (gap OR overlap) is rejected in strict mode
                raise GapDetectedError(f"Initial sync failure: U={U} != last+1 ({self.last_update_id + 1})")
        else:
            # Steady state: Previous u (last_update_id) must be U - 1
            if U != self.last_update_id + 1:
                raise GapDetectedError(f"Gap detected: U={U} != last+1 ({self.last_update_id + 1})")

        # Apply updates
        for price_str, qty_str in update_data.get("b", []):
            self._update_level(self.bids, float(price_str), float(qty_str))
            
        for price_str, qty_str in update_data.get("a", []):
            self._update_level(self.asks, float(price_str), float(qty_str))

        self.last_update_id = u
        return True

    def _update_level(self, side_dict: Dict[float, float], price: float, qty: float):
        if qty == 0.0:
            if price in side_dict:
                del side_dict[price]
        else:
            side_dict[price] = qty

    def get_snapshot_dict(self) -> Dict[str, Any]:
        """Returns the current state in a format similar to the REST snapshot"""
        # Sort bids descending, asks ascending
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])
        
        return {
            "lastUpdateId": self.last_update_id,
            "bids": [[str(p), str(q)] for p, q in sorted_bids],
            "asks": [[str(p), str(q)] for p, q in sorted_asks]
        }

    def get_best_prices(self) -> Dict[str, float | None]:
        """Returns the best bid and ask prices."""
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        return {"best_bid": best_bid, "best_ask": best_ask, "lastUpdateId": self.last_update_id}
