import logging
from pathlib import Path
from .config import LOG_ROOT
from .formatters import get_formatter

def get_logger(
    name: str,
    exchange: str,
    symbol: str | None = None,
    level=logging.INFO,
):
    """
    Create or get a namespaced logger for a specific listener.
    """
    parts = [exchange]
    if symbol:
        parts.append(symbol)

    log_dir = LOG_ROOT.joinpath(*parts)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger_name = f"{exchange}.{symbol}.{name}" if symbol else f"{exchange}.{name}"
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger  # 防止重复添加 handler

    logger.setLevel(level)
    logger.propagate = False

    formatter = get_formatter()

    # File handler
    file_handler = logging.FileHandler(log_dir / f"{name}.log")
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
