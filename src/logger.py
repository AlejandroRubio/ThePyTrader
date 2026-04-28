import logging
import os
from parametrization import LOG_LEVEL, LOG_FILE


def _setup_logging():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)
    if not root.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        root.addHandler(ch)
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)


_setup_logging()


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
