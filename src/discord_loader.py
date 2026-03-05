
import logging
import duckdb
from pathlib import Path

from src.logging_config import get_logger

logger = get_logger(__name__)


class DiscordLoader:
    def __init__(self):
        pass

    def find_messages(self, directory: str) -> list:
        path = Path(directory)
        logger.debug(f"Searching for JSON files in {path}")

        if not path.exists():
            logger.error(f"Directory does not exist: {directory}")
            return []

        files = list(path.rglob("messages.json"))
        logger.info(f"Found {len(files)} JSON files")

        return [str(f) for f in files]

    def read_relation(self, path):
        path = Path(path)
        if not path.exists():
            logger.error(f"Message.json does not exist: {path}")
            return []

        return duckdb.sql(f"SELECT * FROM '{str(path)}'")
