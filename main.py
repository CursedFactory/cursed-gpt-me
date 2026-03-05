import argparse
import logging
import pandas as pd
from tqdm import tqdm
from src.logging_config import get_logger
from src.discord_loader import DiscordLoader

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="GPT Discord Small - Import and process Discord messages")
    parser.add_argument("-i", "--input", required=True, help="Directory containing Discord messages")
    args = parser.parse_args()

    logger.info("Starting gpt-discord-small")

    discord_loader = DiscordLoader()
    path_messages = discord_loader.find_messages(args.input)
    logger.info(f"Found {len(path_messages)} Discord messages")


    msg_df = pd.DataFrame(columns=['ID', 'Timestamp', 'Contents', 'Attachments'])
    for msg_path in tqdm(path_messages, desc="Loading messages"):
        relation = discord_loader.read_relation(msg_path)
        df = relation.to_df()
        msg_df = pd.concat([msg_df, df], ignore_index=True)
    logger.info(f"Loaded {len(msg_df)} total rows")

    print(msg_df.head())


if __name__ == "__main__":
    main()
