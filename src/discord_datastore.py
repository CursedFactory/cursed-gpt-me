from typing import List, Optional, Tuple

import duckdb
from src.logging_config import get_logger
from src.message import Message

logger = get_logger(__name__)


class Datastore:
    def __init__(self, database: str = "datastore.duckdb"):
        self.database = database
        self.connection = None
        self._connect()

    def _connect(self):
        """Connect to the DuckDB database."""
        self.connection = duckdb.connect(database=self.database)
        self._create_table()

    def _create_table(self):
        """Create the messages table if it doesn't exist."""
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id VARCHAR,
                timestamps BIGINT,
                content VARCHAR,
                username VARCHAR,
                platform VARCHAR
            )
        """)

    def add_message(self, message: Message) -> None:
        """Add a single message to the datastore.

        Args:
            message: The Message object to add
        """
        self.connection.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?)",
            message.to_tuple()
        )
        self.connection.commit()

    def add_messages(self, messages: List[Message]) -> None:
        """Add multiple messages to the datastore.

        Args:
            messages: List of Message objects
        """
        if not messages:
            return

        self.connection.executemany(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?)",
            [msg.to_tuple() for msg in messages],
        )
        self.connection.commit()

    def clear_messages(self) -> None:
        """Delete all messages from the datastore."""
        self.connection.execute("DELETE FROM messages")
        self.connection.commit()

    def get_message_count(self) -> int:
        """Get the total number of messages in the datastore.

        Returns:
            The count of messages
        """
        result = self.connection.execute("SELECT COUNT(*) FROM messages").fetchone()
        return result[0] if result else 0

    def get_random_message_and_preceding(self, n: int = 3) -> Tuple[Optional[Message], List[Message]]:
        """Get a random message and the last n messages before it.

        Args:
            n: Number of preceding messages to retrieve (default: 3)

        Returns:
            Tuple of (random_message, preceding_messages)
        """
        dedup_cte = """
            WITH unique_messages AS (
                SELECT id, timestamps, content, username, platform
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY timestamps, content, username, platform
                               ORDER BY id
                           ) AS row_num
                    FROM messages
                )
                WHERE row_num = 1
            )
        """

        # Get a random message from deduplicated rows
        random_result = self.connection.execute(
            dedup_cte + "SELECT * FROM unique_messages ORDER BY RANDOM() LIMIT 1"
        ).fetchone()

        if not random_result:
            return None, []

        # Get the last n messages before the random message
        preceding_result = self.connection.execute(
            dedup_cte + "SELECT * FROM unique_messages WHERE timestamps < ? ORDER BY timestamps DESC LIMIT ?",
            (random_result[1], n)
        ).fetchall()

        # Convert to Message objects
        random_message = Message.from_tuple(random_result)
        preceding_messages = [Message.from_tuple(row) for row in reversed(preceding_result)]

        return random_message, preceding_messages

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
