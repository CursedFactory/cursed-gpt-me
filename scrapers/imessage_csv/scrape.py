import argparse
import csv
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from dotenv import load_dotenv


class DatastoreApiClient:
    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 30) -> None:
        normalized_base = base_url.rstrip("/") + "/"
        self.bulk_url = urljoin(normalized_base, "messages/bulk")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def post_messages_bulk(self, messages: list[dict[str, str]]) -> int:
        if not messages:
            return 0

        payload = {"messages": messages}
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
        }

        max_attempts = 5
        backoff_seconds = 1.0

        for attempt in range(1, max_attempts + 1):
            try:
                request = Request(
                    self.bulk_url, data=body, headers=headers, method="POST"
                )
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    status = response.getcode()
                    raw_response = response.read().decode("utf-8", "replace")

                if 200 <= status < 300:
                    parsed = json.loads(raw_response)
                    inserted = parsed.get("data", {}).get("inserted")
                    if isinstance(inserted, int):
                        return inserted
                    return len(messages)

                raise RuntimeError(f"unexpected status code: {status}")
            except HTTPError as error:
                status = error.code
                detail = error.read().decode("utf-8", "replace")
                retriable = status >= 500 or status == 429
                if not retriable or attempt == max_attempts:
                    raise RuntimeError(
                        f"bulk POST failed with status {status}: {detail}"
                    ) from error
            except URLError as error:
                if attempt == max_attempts:
                    raise RuntimeError(
                        f"bulk POST failed after retries: {error}"
                    ) from error
            except TimeoutError as error:
                if attempt == max_attempts:
                    raise RuntimeError("bulk POST timed out after retries") from error

            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 8.0)

        raise RuntimeError("bulk POST failed after retries")


def find_default_csv_path() -> Path:
    default_dir = Path("~/data/vfp_gdpr/imessage").expanduser()
    if not default_dir.exists():
        raise ValueError(f"Default directory not found: {default_dir}")

    csv_paths = sorted(default_dir.glob("*.csv"))
    if not csv_paths:
        raise ValueError(f"No CSV files found under: {default_dir}")

    return csv_paths[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import iMessage/SMS CSV rows into datastore API as IMESSAGE platform"
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=None,
        help="Path to CSV export file (defaults to first CSV in ~/data/vfp_gdpr/imessage)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Rows per API request (1-500)",
    )
    parser.add_argument(
        "--platform",
        type=str,
        default="IMESSAGE",
        help="Platform label stored in datastore",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap on rows imported (0 means no cap)",
    )
    return parser.parse_args()


def row_to_payload(row: dict[str, str], platform: str) -> dict[str, str] | None:
    message = clean_message_text((row.get("Message") or "").strip())
    if not message:
        return None

    date = (row.get("Date") or "").strip()
    sender = (row.get("Sender") or "Unknown").strip() or "Unknown"
    contact = (row.get("Contact") or "Unknown").strip() or "Unknown"
    service = (row.get("Service") or "").strip()

    user = f"{sender}:{contact}"
    thread = contact if contact != "Unknown" else sender

    body = message

    platform_uuid = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "|".join([date, sender, contact, service, message]),
        )
    )

    return {
        "user": user,
        "thread": thread,
        "platform_uuid": platform_uuid,
        "platform": platform,
        "message": body,
    }


def clean_message_text(message: str) -> str:
    if not message:
        return message

    cleaned = message.strip()
    bracketed_prefix = (
        r"^\[(?:\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?)\](?:\[[^\]]+\])?\s*"
    )
    cleaned = re.sub(bracketed_prefix, "", cleaned)

    plain_prefix = r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:\s*[-|]\s*|\s+)"
    cleaned = re.sub(plain_prefix, "", cleaned)

    return cleaned.strip()


def load_rows(csv_path: Path, platform: str, limit: int) -> list[dict[str, str]]:
    payloads: list[dict[str, str]] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            payload = row_to_payload(row, platform)
            if payload is None:
                continue

            payloads.append(payload)
            if limit > 0 and len(payloads) >= limit:
                break

    return payloads


def chunked(items: list[dict[str, str]], chunk_size: int) -> list[list[dict[str, str]]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def main() -> None:
    args = parse_args()

    if args.batch_size <= 0 or args.batch_size > 500:
        raise ValueError("--batch-size must be between 1 and 500")

    if args.limit < 0:
        raise ValueError("--limit must be >= 0")

    load_dotenv()

    api_base_url = os.getenv("DATASTORE_API_BASE_URL")
    api_key = os.getenv("DATASTORE_API_KEY")

    if not api_base_url:
        raise ValueError("DATASTORE_API_BASE_URL is required")

    if not api_key:
        raise ValueError("DATASTORE_API_KEY is required")

    csv_path = (
        Path(args.csv_path).expanduser() if args.csv_path else find_default_csv_path()
    )
    if not csv_path.exists():
        raise ValueError(f"CSV file not found: {csv_path}")

    print(f"Reading CSV: {csv_path}")
    messages = load_rows(csv_path, args.platform, args.limit)
    print(f"Prepared {len(messages)} message(s) for import")

    client = DatastoreApiClient(api_base_url, api_key)
    total_inserted = 0
    batches = chunked(messages, args.batch_size)

    for index, batch in enumerate(batches, start=1):
        inserted = client.post_messages_bulk(batch)
        total_inserted += inserted
        print(
            f"Posted batch {index}/{len(batches)} size={len(batch)} inserted={inserted} total_inserted={total_inserted}"
        )

    print(f"Done. Inserted {total_inserted} message(s).")


if __name__ == "__main__":
    main()
