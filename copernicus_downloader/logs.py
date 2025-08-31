import logging
import os
from logging import Logger

_loggers: dict[str, Logger] = {}


def get_logger(name: str) -> Logger:
    """
    Return a logger configured with LOG_LEVEL from env.
    Ensures at least one StreamHandler is attached.
    """
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)

    # Determine log level from env
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level)

    # Attach a default handler if none exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        logger.addHandler(handler)

    _loggers[name] = logger
    return logger
