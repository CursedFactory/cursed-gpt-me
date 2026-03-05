import argparse
import math
import os
import re
import shlex
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "datastore.db"
DEFAULT_NANOCHAT_DIR = REPO_ROOT / "approaches" / "nanochat"
DEFAULT_NANOCHAT_BASE_DIR = (
    REPO_ROOT / "training" / "nanochat" / "artifacts" / "nanochat_cache"
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


@dataclass
class ExportStats:
    source_rows: int
    kept_rows: int
    threads_processed: int
    train_documents: int
    validation_documents: int
    train_shards: int
    validation_shard_path: Path
    output_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export SQLite text_messages into nanochat parquet shards, train a tokenizer, "
            "then run nanochat base training."
        ),
    )

    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="Path to datastore SQLite database",
    )
    parser.add_argument(
        "--nanochat-dir",
        type=str,
        default=str(DEFAULT_NANOCHAT_DIR),
        help="Path to the nanochat checkout",
    )
    parser.add_argument(
        "--nanochat-base-dir",
        type=str,
        default=str(DEFAULT_NANOCHAT_BASE_DIR),
        help=(
            "NANOCHAT_BASE_DIR to use. Parquet shards, tokenizer, and checkpoints are written under this directory."
        ),
    )
    parser.add_argument(
        "--python-executable",
        type=str,
        default=sys.executable,
        help="Python executable used to run nanochat scripts",
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
        "--validation-ratio",
        type=float,
        default=0.1,
        help="Tail split ratio per thread reserved for validation documents",
    )
    parser.add_argument(
        "--documents-per-train-shard",
        type=int,
        default=4096,
        help="Maximum number of training documents per parquet shard",
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=1024,
        help="Parquet row group size",
    )
    parser.add_argument(
        "--plain-text",
        action="store_true",
        help="Export simplified chat lines as [USER]: MESSAGE (omit metadata tags)",
    )

    parser.add_argument(
        "--skip-tokenizer",
        action="store_true",
        help="Skip tokenizer training",
    )
    parser.add_argument(
        "--skip-training",
        action="store_true",
        help="Skip nanochat base model training",
    )
    parser.add_argument(
        "--tokenizer-max-chars",
        type=int,
        default=200_000_000,
        help="Maximum characters for tokenizer training",
    )
    parser.add_argument(
        "--tokenizer-doc-cap",
        type=int,
        default=10_000,
        help="Maximum characters per document when training tokenizer",
    )
    parser.add_argument(
        "--vocab-size",
        type=int,
        default=16_384,
        help="Tokenizer vocabulary size",
    )

    parser.add_argument(
        "--run",
        type=str,
        default="auto",
        help="Run name for tracker/checkpoints. Use 'auto' for timestamped naming.",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="",
        help="Optional short label used as run name when --run=auto (e.g. simplerprompt)",
    )
    parser.add_argument(
        "--tracker",
        type=str,
        default="aim",
        choices=["aim", "wandb", "none"],
        help="metrics tracker backend used by scripts.base_train",
    )
    parser.add_argument(
        "--device-type",
        type=str,
        default="",
        help="cuda|cpu|mps (empty means nanochat autodetect)",
    )
    parser.add_argument("--depth", type=int, default=6)
    parser.add_argument("--head-dim", type=int, default=64)
    parser.add_argument("--window-pattern", type=str, default="L")
    parser.add_argument("--max-seq-len", type=int, default=512)
    parser.add_argument("--device-batch-size", type=int, default=8)
    parser.add_argument("--total-batch-size", type=int, default=8192)
    parser.add_argument("--num-iterations", type=int, default=1000)
    parser.add_argument("--eval-every", type=int, default=100)
    parser.add_argument("--eval-tokens", type=int, default=131_072)
    parser.add_argument("--core-metric-every", type=int, default=-1)
    parser.add_argument("--sample-every", type=int, default=-1)
    parser.add_argument("--save-every", type=int, default=-1)
    parser.add_argument(
        "--base-train-arg",
        action="append",
        default=[],
        help=(
            "Extra raw argument forwarded to scripts.base_train, e.g. "
            "--base-train-arg=--warmup-ratio=0.03"
        ),
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
            "--min-messages-per-sample cannot exceed --messages-per-sample",
        )
    if args.min_chars < 0:
        raise ValueError("--min-chars must be >= 0")
    if not (0 <= args.validation_ratio < 1):
        raise ValueError("--validation-ratio must be between 0 (inclusive) and 1")
    if args.documents_per_train_shard <= 0:
        raise ValueError("--documents-per-train-shard must be > 0")
    if args.row_group_size <= 0:
        raise ValueError("--row-group-size must be > 0")

    if args.vocab_size <= 0:
        raise ValueError("--vocab-size must be > 0")
    if args.tokenizer_max_chars <= 0:
        raise ValueError("--tokenizer-max-chars must be > 0")
    if args.tokenizer_doc_cap <= 0:
        raise ValueError("--tokenizer-doc-cap must be > 0")

    if args.depth <= 0:
        raise ValueError("--depth must be > 0")
    if args.head_dim <= 0:
        raise ValueError("--head-dim must be > 0")
    if args.max_seq_len <= 0:
        raise ValueError("--max-seq-len must be > 0")
    if args.device_batch_size <= 0:
        raise ValueError("--device-batch-size must be > 0")
    if args.total_batch_size <= 0:
        raise ValueError("--total-batch-size must be > 0")
    if args.num_iterations <= 0:
        raise ValueError("--num-iterations must be > 0")
    if args.eval_tokens <= 0:
        raise ValueError("--eval-tokens must be > 0")

    return args


def normalize_message(message: str) -> str:
    normalized = message.replace("\r\n", "\n").replace("\r", "\n").strip()
    normalized = WHITESPACE_RE.sub(" ", normalized)
    normalized = MULTI_NEWLINE_RE.sub("\n\n", normalized)
    return normalized


def format_message(row: MessageRow, include_metadata: bool) -> str:
    if not include_metadata:
        return f"[{row.user}]: {row.message}"

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
        yield MessageRow(
            id=int(raw_row[0]),
            user=str(raw_row[1]),
            thread=str(raw_row[2]),
            platform_uuid=str(raw_row[3]),
            platform=str(raw_row[4]),
            message=str(raw_row[5]),
            created_at=str(raw_row[6]),
        )


def build_document(thread_rows: list[MessageRow], include_metadata: bool) -> str:
    rendered_messages = [format_message(row, include_metadata) for row in thread_rows]
    return f"\n{MESSAGE_BREAK_TOKEN}\n".join(rendered_messages)


def build_documents_for_thread(
    thread_rows: list[MessageRow],
    messages_per_sample: int,
    sample_overlap: int,
    min_messages_per_sample: int,
    include_metadata: bool,
) -> list[str]:
    if not thread_rows:
        return []

    step = messages_per_sample - sample_overlap
    documents: list[str] = []
    emitted_tail = False

    for start in range(0, len(thread_rows), step):
        chunk = thread_rows[start : start + messages_per_sample]
        if len(chunk) < min_messages_per_sample:
            break

        documents.append(build_document(chunk, include_metadata))
        if start + messages_per_sample >= len(thread_rows):
            emitted_tail = True
            break

    if not emitted_tail:
        tail_chunk = thread_rows[-messages_per_sample:]
        if len(tail_chunk) >= min_messages_per_sample:
            documents.append(build_document(tail_chunk, include_metadata))

    return documents


def split_thread_documents(
    documents: list[str],
    validation_ratio: float,
) -> tuple[list[str], list[str]]:
    if not documents:
        return [], []

    if validation_ratio <= 0 or len(documents) < 2:
        return documents, []

    val_count = max(1, math.ceil(len(documents) * validation_ratio))
    if val_count >= len(documents):
        val_count = 1

    train_count = len(documents) - val_count
    return documents[:train_count], documents[train_count:]


def iter_chunks(items: list[str], chunk_size: int) -> Iterable[list[str]]:
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def write_parquet_documents(
    documents: list[str],
    output_path: Path,
    row_group_size: int,
) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pyarrow is required. Activate the nanochat virtualenv (approaches/nanochat/.venv) and try again.",
        ) from exc

    if not documents:
        raise ValueError(f"Cannot write empty parquet document list: {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    effective_row_group_size = max(1, min(row_group_size, len(documents)))
    table = pa.Table.from_pydict({"text": documents})
    pq.write_table(
        table,
        str(output_path),
        row_group_size=effective_row_group_size,
        use_dictionary=False,
        compression="zstd",
        compression_level=3,
        write_statistics=False,
    )


def clear_existing_shards(output_dir: Path) -> None:
    for path in output_dir.glob("shard_*.parquet"):
        path.unlink()
    for path in output_dir.glob("shard_*.parquet.tmp"):
        path.unlink()


def export_sqlite_to_parquet(args: argparse.Namespace, output_dir: Path) -> ExportStats:
    db_path = Path(args.db_path).expanduser().resolve()
    if not db_path.exists():
        raise ValueError(f"Database file not found: {db_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    clear_existing_shards(output_dir)

    include_metadata = not args.plain_text

    source_rows = 0
    kept_rows = 0
    threads_processed = 0
    train_documents: list[str] = []
    validation_documents: list[str] = []

    current_thread: str | None = None
    current_thread_rows: list[MessageRow] = []

    def flush_current_thread() -> None:
        nonlocal threads_processed, current_thread_rows
        if not current_thread_rows:
            return

        threads_processed += 1
        documents = build_documents_for_thread(
            thread_rows=current_thread_rows,
            messages_per_sample=args.messages_per_sample,
            sample_overlap=args.sample_overlap,
            min_messages_per_sample=args.min_messages_per_sample,
            include_metadata=include_metadata,
        )
        train_docs, val_docs = split_thread_documents(
            documents,
            args.validation_ratio,
        )
        train_documents.extend(train_docs)
        validation_documents.extend(val_docs)
        current_thread_rows = []

    with sqlite3.connect(db_path) as connection:
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
                flush_current_thread()
                current_thread = normalized_row.thread

            current_thread_rows.append(normalized_row)

    flush_current_thread()

    if not train_documents and not validation_documents:
        raise ValueError("No documents were produced from the SQLite dataset")

    if not validation_documents:
        if len(train_documents) > 1:
            validation_documents.append(train_documents.pop())
        else:
            validation_documents.append(train_documents[0])

    if not train_documents:
        train_documents.append(validation_documents[0])

    train_chunks = list(iter_chunks(train_documents, args.documents_per_train_shard))
    train_shard_count = len(train_chunks)
    for shard_index, chunk in enumerate(train_chunks):
        shard_path = output_dir / f"shard_{shard_index:05d}.parquet"
        write_parquet_documents(chunk, shard_path, args.row_group_size)

    validation_shard_index = train_shard_count
    validation_shard_path = output_dir / f"shard_{validation_shard_index:05d}.parquet"
    write_parquet_documents(
        validation_documents, validation_shard_path, args.row_group_size
    )

    return ExportStats(
        source_rows=source_rows,
        kept_rows=kept_rows,
        threads_processed=threads_processed,
        train_documents=len(train_documents),
        validation_documents=len(validation_documents),
        train_shards=train_shard_count,
        validation_shard_path=validation_shard_path,
        output_dir=output_dir,
    )


def run_command(command: list[str], workdir: Path, env: dict[str, str]) -> None:
    rendered = " ".join(shlex.quote(part) for part in command)
    print(f"\n$ {rendered}")
    subprocess.run(command, cwd=str(workdir), env=env, check=True)


def build_tok_train_command(args: argparse.Namespace) -> list[str]:
    return [
        args.python_executable,
        "-m",
        "scripts.tok_train",
        f"--max-chars={args.tokenizer_max_chars}",
        f"--doc-cap={args.tokenizer_doc_cap}",
        f"--vocab-size={args.vocab_size}",
    ]


def build_base_train_command(args: argparse.Namespace) -> list[str]:
    command = [
        args.python_executable,
        "-m",
        "scripts.base_train",
        f"--run={args.run}",
        f"--tracker={args.tracker}",
        f"--depth={args.depth}",
        f"--head-dim={args.head_dim}",
        f"--window-pattern={args.window_pattern}",
        f"--max-seq-len={args.max_seq_len}",
        f"--device-batch-size={args.device_batch_size}",
        f"--total-batch-size={args.total_batch_size}",
        f"--num-iterations={args.num_iterations}",
        f"--eval-every={args.eval_every}",
        f"--eval-tokens={args.eval_tokens}",
        f"--core-metric-every={args.core_metric_every}",
        f"--sample-every={args.sample_every}",
        f"--save-every={args.save_every}",
    ]

    if args.device_type:
        command.append(f"--device-type={args.device_type}")

    command.extend(args.base_train_arg)
    return command


def resolve_run_name(args: argparse.Namespace) -> str:
    run = args.run.strip()
    if run and run.lower() != "auto":
        return run

    label = args.label.strip()
    if label:
        return label

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"sqlite-nanochat-d{args.depth}-s{args.max_seq_len}-{timestamp}"


def verify_paths(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    db_path = Path(args.db_path).expanduser().resolve()
    if not db_path.exists():
        raise ValueError(f"Database not found: {db_path}")

    nanochat_dir = Path(args.nanochat_dir).expanduser().resolve()
    if not nanochat_dir.exists():
        raise ValueError(f"nanochat directory not found: {nanochat_dir}")

    required_script_paths = [
        nanochat_dir / "scripts" / "tok_train.py",
        nanochat_dir / "scripts" / "base_train.py",
    ]
    for path in required_script_paths:
        if not path.exists():
            raise ValueError(f"Required nanochat script not found: {path}")

    nanochat_base_dir = Path(args.nanochat_base_dir).expanduser().resolve()
    nanochat_base_dir.mkdir(parents=True, exist_ok=True)

    return db_path, nanochat_dir, nanochat_base_dir


def main() -> None:
    args = parse_args()
    args.run = resolve_run_name(args)
    db_path, nanochat_dir, nanochat_base_dir = verify_paths(args)
    output_dir = nanochat_base_dir / "base_data_climbmix"

    print("Preparing nanochat parquet shards from SQLite...")
    stats = export_sqlite_to_parquet(args, output_dir)
    print(f"SQLite DB: {db_path}")
    print(f"Rows scanned: {stats.source_rows}")
    print(f"Rows kept: {stats.kept_rows}")
    print(f"Threads processed: {stats.threads_processed}")
    print(f"Train documents: {stats.train_documents}")
    print(f"Validation documents: {stats.validation_documents}")
    print(f"Train shards written: {stats.train_shards}")
    print(f"Validation shard written: {stats.validation_shard_path}")
    print(f"Run name: {args.run}")

    env = os.environ.copy()
    env["NANOCHAT_BASE_DIR"] = str(nanochat_base_dir)

    if not args.skip_tokenizer:
        run_command(build_tok_train_command(args), nanochat_dir, env)
    else:
        print("Skipping tokenizer training (--skip-tokenizer)")

    if not args.skip_training:
        run_command(build_base_train_command(args), nanochat_dir, env)
    else:
        print("Skipping base model training (--skip-training)")

    print("\nDone.")
    print(f"NANOCHAT_BASE_DIR: {nanochat_base_dir}")
    print(f"Dataset shard directory: {output_dir}")


if __name__ == "__main__":
    main()
