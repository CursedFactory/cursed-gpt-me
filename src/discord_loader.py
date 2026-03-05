
import logging
import duckdb
import pandas as pd
from pathlib import Path

from src.logging_config import get_logger
from src.message import Message

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
            return None

        relation = duckdb.sql(f"SELECT * FROM '{str(path)}'")

        if relation.fetchone() is None:
            return None

        return relation

    def process_messages(self, relation, batch_size=1000):
        messages = []

        if relation is None:
            logger.warning("No data to process")
            return messages

        relation_list = relation.fetchall()

        for i, row in enumerate(relation_list):
            message_id = i
            if isinstance(row[1], str):
                timestamps = int(row[1].replace(' ', '').replace(':', ''))
            else:
                timestamps = int(row[1].timestamp())
            content = str(row[2])
            username = "Unknown"
            platform = 'DISCORD_DM'

            messages.append(Message(
                id=message_id,
                timestamps=timestamps,
                content=content,
                username=username,
                platform=platform
            ))

        return messages

    def _detect_platform(self, row):
        platform = 'UNKNOWN'
        if 'discord' in str(row).lower():
            platform = 'DISCORD_DM'
        return platform
