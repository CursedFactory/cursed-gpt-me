import argparse

from tqdm import tqdm
from src.logging_config import get_logger
from src.discord_loader import DiscordLoader
from src.discord_datastore import Datastore

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="GPT Discord Small - Import and process Discord messages")
    parser.add_argument("-i", "--input", required=True, help="Directory containing Discord messages")
    parser.add_argument("-n", "--limit", type=int, default=100, help="Limit number of messages to process (default: 100)")
    args = parser.parse_args()

    logger.info("Starting gpt-discord-small")

    discord_loader = DiscordLoader()
    path_messages = discord_loader.find_messages(args.input)
    logger.info(f"Found {len(path_messages)} Discord messages")

    if not path_messages:
        return

    datastore = Datastore()
    try:
        for i, msg_path in enumerate(tqdm(path_messages, desc="Loading messages")):
            if i >= args.limit:
                break
            relation = discord_loader.read_relation(msg_path)
            if relation is not None:
                messages = discord_loader.process_messages(relation)
                datastore.add_messages(messages)

    finally:
        total = datastore.get_message_count()
        datastore.close()
        logger.info(f"Total messages loaded: {total}")

    # Test the new get_random_message_and_preceding method
    datastore = Datastore()
    test_random_message(datastore)
    datastore.close()


def test_random_message(datastore: Datastore) -> None:
    """Test the get_random_message_and_preceding method."""
    logger.info("Testing get_random_message_and_preceding method...")

    random_message, preceding_messages = datastore.get_random_message_and_preceding(n=3)

    if not random_message:
        logger.warning("No messages found in database to test")
        return

    logger.info(f"Random message:")
    logger.info(f"  ID: {random_message.id}")
    logger.info(f"  Username: {random_message.username}")
    logger.info(f"  Content: {random_message.content[:50]}...")
    logger.info(f"  Timestamps: {random_message.timestamps}")

    logger.info(f"Preceding messages ({len(preceding_messages)}):")
    for i, msg in enumerate(preceding_messages, 1):
        logger.info(f"  {i}. {msg.username}: {msg.content[:50]}... (ts: {msg.timestamps})")

    logger.info("Test completed successfully")


if __name__ == "__main__":
    main()
