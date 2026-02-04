from typing import Protocol, List
import time
import signal
import sys
from lob_data_engine.logging.factory import get_logger

# Configure logging
logger = get_logger(name="manager", exchange="ws")

class WSListener(Protocol):
    exchange: str

    def start(self) -> None: ...
    def stop(self) -> None: ...
    def is_alive(self) -> bool: ...

class WSListenerManager:
    def __init__(self, listeners: List[WSListener]):
        self.listeners = listeners
        self.running = False

    def start_all(self):
        logger.info("Starting all listeners...")
        self.running = True
        for listener in self.listeners:
            logger.info(f"Starting {listener.exchange} listener...")
            listener.start()

    def stop_all(self):
        logger.info("Stopping all listeners...")
        self.running = False
        for listener in self.listeners:
            logger.info(f"Stopping {listener.exchange} listener...")
            listener.stop()

    def run_forever(self):
        """
        Keeps the main thread alive and handles signals to stop.
        """
        def handle_signal(sig, frame):
            logger.info("Signal received, stopping...")
            self.stop_all()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        self.start_all()

        try:
            while self.running:
                all_dead = True
                for listener in self.listeners:
                    if listener.is_alive():
                        all_dead = False
                    else:
                        logger.warning(f"{listener.exchange} listener is not alive.")
                
                if all_dead and self.listeners:
                    logger.error("All listeners are dead. Exiting.")
                    break
                    
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_all()
        finally:
            logger.info("Manager exit.")
