import argparse

from tqdm import tqdm

from src.discord_datastore import Datastore
from src.discord_loader import DiscordLoader
from src.logging_config import get_logger

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Discord messages into datastore")
    parser.add_argument("-i", "--input", required=True, help="Directory containing Discord messages")
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=100,
        help="Limit number of message files to process (default: 100)",
    )
    args = parser.parse_args()

    logger.info("Starting loader")
    discord_loader = DiscordLoader()
    path_messages = discord_loader.find_messages(args.input)
    logger.info(f"Found {len(path_messages)} Discord messages")

    if not path_messages:
        return

    datastore = Datastore()
    try:
        datastore.clear_messages()
        for i, msg_path in enumerate(tqdm(path_messages, desc="Loading messages")):
            if i >= args.limit:
                break

            relation = discord_loader.read_relation(msg_path)
            if relation is None:
                continue

            messages = discord_loader.process_messages(relation)
            datastore.add_messages(messages)
    finally:
        total = datastore.get_message_count()
        datastore.close()
        logger.info(f"Total messages loaded: {total}")


if __name__ == "__main__":
    main()
