import argparse
import logging

from src.logging_config import get_logger
from src.discord_loader import DiscordLoader

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="GPT Discord Small - Import and process Discord messages")
    parser.add_argument("-i", "--input", required=True, help="Directory containing Discord messages")
    args = parser.parse_args()

    logger.info("Starting gpt-discord-small")

    discord_loader = DiscordLoader()
    messages = discord_loader.discord_find_messages(args.input)
    logger.info(f"Found {len(messages)} Discord messages")




if __name__ == "__main__":
    main()
