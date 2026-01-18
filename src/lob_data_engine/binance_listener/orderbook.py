from typing import Dict, Optional
from lob_data_engine.logging.factory import get_logger
from lob_data_engine.raw.schemas import BinanceDiff


class GapDetectedError(Exception):
    """Raised when a sequence gap is detected in the order book stream."""
    pass

class StreamSequenceVerifier:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.last_update_id: Optional[int] = None
        self.logger = get_logger("verifier", "Binance", symbol)

    def process_update(self, update_data: BinanceDiff) -> bool:
        """
        Check sequence of depthUpdate event.
        Returns True if sequence is valid.
        Raises GapDetectedError if a sequence gap is detected.
        """
        u = update_data["u"] # Final update ID
        U = update_data["U"] # First update ID

        if self.last_update_id is None:
            # First message seen, initialize sequence tracking
            self.last_update_id = u
            self.logger.info(f"[{self.symbol}] Sequence tracking started. Initial lastUpdateId: {u}")
            return True

        # Sequence Check: U must be last_update_id + 1
        if U != self.last_update_id + 1:
            # Check if this is a redundant message (old data)
            if u <= self.last_update_id:
                return False # Ignore old message
            
            # If U > last + 1, it's a gap
            raise GapDetectedError(f"Gap detected: U={U} != last+1 ({self.last_update_id + 1})")

        self.last_update_id = u
        return True