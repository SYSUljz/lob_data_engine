import logging

DEFAULT_FORMAT = (
    "%(asctime)s | %(levelname)s | "
    "%(process)d | %(name)s | %(message)s"
)

def get_formatter():
    return logging.Formatter(DEFAULT_FORMAT)

