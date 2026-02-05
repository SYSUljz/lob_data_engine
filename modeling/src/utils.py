import logging
import time
import sys
from contextlib import contextmanager

def setup_logger(name: str) -> logging.Logger:
    """
    Sets up a logger with standard formatting.
    Output: Stream (Console).
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if setup is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger

@contextmanager
def timer(description: str, logger: logging.Logger = None):
    """
    Context manager to time a block of code.
    """
    start = time.time()
    if logger:
        logger.info(f"Starting: {description}...")
    else:
        print(f"Starting: {description}...")
        
    try:
        yield
    finally:
        elapsed = time.time() - start
        msg = f"Finished: {description} in {elapsed:.2f}s"
        if logger:
            logger.info(msg)
        else:
            print(msg)
