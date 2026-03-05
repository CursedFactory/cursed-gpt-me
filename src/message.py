import json
from dataclasses import dataclass
from typing import List


@dataclass
class Message:
    """Represents a Discord message with the database schema."""
    id: str
    timestamps: int
    content: str
    username: str
    platform: str

    @classmethod
    def from_tuple(cls, msg_tuple: tuple) -> 'Message':
        """Create a Message from a database tuple."""
        return cls(
            id=str(msg_tuple[0]),
            timestamps=msg_tuple[1],
            content=msg_tuple[2],
            username=msg_tuple[3],
            platform=msg_tuple[4]
        )

    def to_tuple(self) -> tuple:
        """Convert Message to a database tuple."""
        return (str(self.id), self.timestamps, self.content, self.username, self.platform)


def format_textfile(
    messages: List[Message],
    context_window: int = 3,
    mode: str = 'training'
) -> str:
    """Format messages for GPT-2 training or prompt generation.

    Args:
        messages: List of Message objects
        context_window: Number of previous messages to use as context
        mode: 'training' or 'prompt' mode

    Returns:
        Formatted string with context and target message
    """
    if mode not in ['training', 'prompt']:
        raise ValueError("mode must be either 'training' or 'prompt'")

    if not messages:
        return ""

    if len(messages) == 1:
        context_messages: List[Message] = []
        target = messages[0].content
    else:
        target = messages[-1].content
        context_messages = messages[:-1]
        if context_window > 0:
            context_messages = context_messages[-context_window:]
        else:
            context_messages = []

    context = "\n".join(msg.content.strip() for msg in context_messages if msg.content.strip())
    target = target.strip()

    if mode == 'training':
        text = f"<|context|>\n{context}\n<|next|>\n{target}<|end|>\n"
    else:
        text = f"<|context|>\n{context}\n<|next|>\n"

    return json.dumps({"text": text}, ensure_ascii=False)
