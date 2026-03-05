from dataclasses import dataclass
from typing import Optional


@dataclass
class Message:
    """Represents a Discord message with the database schema."""
    id: int
    timestamps: int
    content: str
    username: str
    platform: str

    @classmethod
    def from_tuple(cls, msg_tuple: tuple) -> 'Message':
        """Create a Message from a database tuple."""
        return cls(
            id=msg_tuple[0],
            timestamps=msg_tuple[1],
            content=msg_tuple[2],
            username=msg_tuple[3],
            platform=msg_tuple[4]
        )

    def to_tuple(self) -> tuple:
        """Convert Message to a database tuple."""
        return (self.id, self.timestamps, self.content, self.username, self.platform)