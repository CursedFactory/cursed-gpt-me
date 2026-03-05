import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "datastore.db"
DEFAULT_OUTPUT_PATH = (
    REPO_ROOT / "training" / "gpt2" / "data" / "processed" / "messages.jsonl"
)
MESSAGE_BREAK_TOKEN = "<|message_break|>"
WHITESPACE_RE = re.compile(r"[ \t]+")
MULTI_NEWLINE_RE = re.compile(r"\n{3,}")


@dataclass
class MessageRow:
    id: int
    user: str
    thread: str
    platform_uuid: str
    platform: str
    message: str
    created_at: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export datastore SQLite messages into GPT-2 JSONL training samples",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="Path to SQLite datastore database",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output JSONL path",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap on source rows (0 means no cap)",
    )
    parser.add_argument(
        "--messages-per-sample",
        type=int,
        default=16,
        help="Conversation messages per exported sample window",
    )
    parser.add_argument(
        "--sample-overlap",
        type=int,
        default=4,
        help="Message overlap between consecutive sample windows",
    )
    parser.add_argument(
        "--min-messages-per-sample",
        type=int,
        default=2,
        help="Minimum messages required to emit a sample",
    )
    parser.add_argument(
        "--min-chars",
        type=int,
        default=1,
        help="Minimum message character count after normalization",
    )
    parser.add_argument(
        "--plain-text",
        action="store_true",
        help="Export plain message text only (omit metadata tags)",
    )
    args = parser.parse_args()

    if args.limit < 0:
        raise ValueError("--limit must be >= 0")

    if args.messages_per_sample <= 0:
        raise ValueError("--messages-per-sample must be > 0")

    if args.sample_overlap < 0:
        raise ValueError("--sample-overlap must be >= 0")

    if args.sample_overlap >= args.messages_per_sample:
        raise ValueError("--sample-overlap must be smaller than --messages-per-sample")

    if args.min_messages_per_sample <= 0:
        raise ValueError("--min-messages-per-sample must be > 0")

    if args.min_messages_per_sample > args.messages_per_sample:
        raise ValueError(
            "--min-messages-per-sample cannot exceed --messages-per-sample"
        )

    if args.min_chars < 0:
        raise ValueError("--min-chars must be >= 0")

    return args


def normalize_message(message: str) -> str:
    normalized = message.replace("\r\n", "\n").replace("\r", "\n").strip()
    normalized = WHITESPACE_RE.sub(" ", normalized)
    normalized = MULTI_NEWLINE_RE.sub("\n\n", normalized)
    return normalized


def format_message(row: MessageRow, include_metadata: bool) -> str:
    if not include_metadata:
        return row.message

    return "\n".join(
        [
            f"<|platform|>{row.platform}",
            f"<|thread|>{row.thread}",
            f"<|user|>{row.user}",
            row.message,
        ],
    )


def query_rows(connection: sqlite3.Connection, limit: int) -> Iterable[MessageRow]:
    query = """
    SELECT id, user, thread, platform_uuid, platform, message, createdAt
    FROM text_messages
    WHERE message IS NOT NULL
      AND length(trim(message)) > 0
    ORDER BY thread ASC, createdAt ASC, id ASC
  """

    params: tuple[int, ...] = ()
    if limit > 0:
        query = f"{query}\nLIMIT ?"
        params = (limit,)

    cursor = connection.execute(query, params)
    for raw_row in cursor:
        row = MessageRow(
            id=int(raw_row[0]),
            user=str(raw_row[1]),
            thread=str(raw_row[2]),
            platform_uuid=str(raw_row[3]),
            platform=str(raw_row[4]),
            message=str(raw_row[5]),
            created_at=str(raw_row[6]),
        )
        yield row


def build_sample_record(
    thread_rows: list[MessageRow], include_metadata: bool
) -> dict[str, object]:
    rendered_messages = [format_message(row, include_metadata) for row in thread_rows]
    text = f"\n{MESSAGE_BREAK_TOKEN}\n".join(rendered_messages)

    return {
        "text": text,
        "thread": thread_rows[0].thread,
        "platform": thread_rows[0].platform,
        "message_count": len(thread_rows),
        "start_id": thread_rows[0].id,
        "end_id": thread_rows[-1].id,
        "start_created_at": thread_rows[0].created_at,
        "end_created_at": thread_rows[-1].created_at,
    }


def build_samples_for_thread(
    thread_rows: list[MessageRow],
    messages_per_sample: int,
    sample_overlap: int,
    min_messages_per_sample: int,
    include_metadata: bool,
) -> list[dict[str, object]]:
    if not thread_rows:
        return []

    step = messages_per_sample - sample_overlap
    samples: list[dict[str, object]] = []
    emitted_tail = False

    for start in range(0, len(thread_rows), step):
        chunk = thread_rows[start : start + messages_per_sample]
        if len(chunk) < min_messages_per_sample:
            break

        samples.append(build_sample_record(chunk, include_metadata))
        if start + messages_per_sample >= len(thread_rows):
            emitted_tail = True
            break

    if not emitted_tail:
        tail_chunk = thread_rows[-messages_per_sample:]
        if len(tail_chunk) >= min_messages_per_sample:
            samples.append(build_sample_record(tail_chunk, include_metadata))

    return samples


def main() -> None:
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    output_path = Path(args.output_path).expanduser().resolve()

    if not db_path.exists():
        raise ValueError(f"Database file not found: {db_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    include_metadata = not args.plain_text

    source_rows = 0
    kept_rows = 0
    thread_count = 0
    sample_count = 0

    current_thread: str | None = None
    current_thread_rows: list[MessageRow] = []

    with (
        sqlite3.connect(db_path) as connection,
        output_path.open("w", encoding="utf-8") as output_file,
    ):
        for row in query_rows(connection, args.limit):
            source_rows += 1
            normalized = normalize_message(row.message)

            if len(normalized) < args.min_chars:
                continue

            kept_rows += 1
            normalized_row = MessageRow(
                id=row.id,
                user=row.user,
                thread=row.thread,
                platform_uuid=row.platform_uuid,
                platform=row.platform,
                message=normalized,
                created_at=row.created_at,
            )

            if current_thread is None:
                current_thread = normalized_row.thread

            if normalized_row.thread != current_thread:
                thread_count += 1
                samples = build_samples_for_thread(
                    thread_rows=current_thread_rows,
                    messages_per_sample=args.messages_per_sample,
                    sample_overlap=args.sample_overlap,
                    min_messages_per_sample=args.min_messages_per_sample,
                    include_metadata=include_metadata,
                )
                for sample in samples:
                    output_file.write(json.dumps(sample, ensure_ascii=False) + "\n")
                    sample_count += 1

                current_thread = normalized_row.thread
                current_thread_rows = []

            current_thread_rows.append(normalized_row)

        if current_thread_rows:
            thread_count += 1
            samples = build_samples_for_thread(
                thread_rows=current_thread_rows,
                messages_per_sample=args.messages_per_sample,
                sample_overlap=args.sample_overlap,
                min_messages_per_sample=args.min_messages_per_sample,
                include_metadata=include_metadata,
            )
            for sample in samples:
                output_file.write(json.dumps(sample, ensure_ascii=False) + "\n")
                sample_count += 1

    print(f"Export complete: {output_path}")
    print(f"Rows scanned: {source_rows}")
    print(f"Rows kept: {kept_rows}")
    print(f"Threads processed: {thread_count}")
    print(f"Samples written: {sample_count}")


if __name__ == "__main__":
    main()
