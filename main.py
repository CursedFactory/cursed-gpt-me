import argparse
import logging
from tqdm import tqdm
from src.logging_config import get_logger
from src.discord_loader import DiscordLoader
from src.discord_datastore import Datastore

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="GPT Discord Small - Import and process Discord messages")
    parser.add_argument("-i", "--input", required=True, help="Directory containing Discord messages")
    args = parser.parse_args()

    logger.info("Starting gpt-discord-small")

    discord_loader = DiscordLoader()
    path_messages = discord_loader.find_messages(args.input)
    logger.info(f"Found {len(path_messages)} Discord messages")

    if not path_messages:
        return

    datastore = Datastore()
    try:
        for msg_path in tqdm(path_messages, desc="Loading messages"):
            relation = discord_loader.read_relation(msg_path)
            if relation is not None:
                messages = discord_loader.process_messages(relation)
                datastore.add_messages(messages)
    finally:
        total = datastore.get_message_count()
        datastore.close()
        logger.info(f"Total messages loaded: {total}")


if __name__ == "__main__":
    main()