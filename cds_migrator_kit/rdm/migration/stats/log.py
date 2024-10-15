import logging
import os
from logging import FileHandler
from .config import ROOT_PATH


formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s")


def setup_logger(name, filename, level=logging.INFO):
    """Setup statistics migration logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    filepath = os.path.join(ROOT_PATH, filename)
    handler1 = FileHandler(filepath)
    handler1.setFormatter(formatter)

    logger.addHandler(handler1)

    return logger
