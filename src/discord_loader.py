
import logging
from pathlib import Path

from src.logging_config import get_logger

logger = get_logger(__name__)


class DiscordLoader:
    def __init__(self):
        pass

    def discord_find_messages(self, directory: str) -> list:
        """
        Find all .json files recursively under a given directory.
        Exmaple: /Volumes/PortaOne/Datasets/discord_gdpr/Messages/c85338836384628736/messages.json
        Args:
            directory (str): The root directory to search in

        Returns:
            list: List of file paths matching *.json pattern
        """
        path = Path(directory)
        logger.debug(f"Searching for JSON files in {path}")

        if not path.exists():
            logger.error(f"Directory does not exist: {directory}")
            return []

        files = list(path.rglob("messages.json"))
        logger.info(f"Found {len(files)} JSON files")

        return [str(f) for f in files]
