import duckdb
from typing import List, Optional
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
            DROP TABLE IF EXISTS messages
        """)
        self.connection.execute("""
            CREATE TABLE messages (
                id INTEGER,
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

    def add_messages(self, messages: List[Message]) -> None:
        """Add multiple messages to the datastore.

        Args:
            messages: List of Message objects
        """
        for msg in messages:
            self.add_message(msg)

    def get_message_count(self) -> int:
        """Get the total number of messages in the datastore.

        Returns:
            The count of messages
        """
        result = self.connection.execute("SELECT COUNT(*) FROM messages").fetchone()
        return result[0] if result else 0

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