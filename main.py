import argparse

from tqdm import tqdm
from src.logging_config import get_logger
from src.discord_loader import DiscordLoader
from src.discord_datastore import Datastore
from src.message import format_textfile

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
        datastore.clear_messages()
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

    # Test message formatting
    datastore = Datastore()
    test_formatting(datastore)
    datastore.close()


def test_formatting(datastore: Datastore) -> None:
    """Test formatting messages for training and prompt generation."""
    logger.info("Testing message formatting for training and prompt generation...")

    for i in range(5):
        random_message, preceding_messages = datastore.get_random_message_and_preceding(n=3)

        if not random_message:
            logger.warning("No messages found in database to test")
            return

        message_sequence = preceding_messages + [random_message]
        train_string = format_textfile(message_sequence, context_window=3, mode='training')
        prompt_string = format_textfile(message_sequence, context_window=3, mode='prompt')

        logger.info(f"\n--- Test {i + 1} ---")
        logger.info(f"Training format:\n{train_string}")
        logger.info(f"\nPrompt format:\n{prompt_string}")

    logger.info("Formatting test completed successfully")


if __name__ == "__main__":
    main()
