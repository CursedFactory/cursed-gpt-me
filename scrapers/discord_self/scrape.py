import argparse
import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

import discord
from dotenv import load_dotenv


def log(message: str) -> None:
    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"[{timestamp}] {message}", flush=True)


def parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(
            "datetime must include timezone (example: 2026-01-01T00:00:00+00:00)"
        )

    return parsed


@dataclass
class Config:
    token: str
    api_base_url: str
    api_key: str
    channel_ids: list[int]
    limit: int
    batch_size: int
    include_bots: bool
    before: datetime | None
    after: datetime | None
    platform: str
    state_file: str | None
    list_channels: bool
    dm_only: bool
    dm_max_channels: int | None
    request_delay_seconds: float
    shard_index: int | None
    shard_count: int | None


class StateStore:
    def __init__(self, path: str | None) -> None:
        self.path = Path(path).expanduser() if path else None
        self._state: dict[str, str] = {}
        if self.path:
            self._state = self._load()

    def _load(self) -> dict[str, str]:
        if not self.path or not self.path.exists():
            return {}

        raw = self.path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return {}

        result: dict[str, str] = {}
        for key, value in parsed.items():
            if isinstance(key, str) and isinstance(value, str):
                result[key] = value

        log(f"Loaded state file with {len(result)} channel checkpoint(s): {self.path}")
        return result

    def get_after(self, channel_id: int, cli_after: datetime | None) -> datetime | None:
        from_state = self._state.get(str(channel_id))
        if from_state is None:
            return cli_after

        state_after = parse_datetime(from_state)
        if state_after is None:
            return cli_after

        if cli_after is None:
            return state_after

        return max(cli_after, state_after)

    def update(self, channel_id: int, latest_timestamp: datetime) -> None:
        if not self.path:
            return

        self._state[str(channel_id)] = latest_timestamp.isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(self._state, indent=2), encoding="utf-8")
        tmp_path.replace(self.path)
        log(
            f"Updated checkpoint channel_id={channel_id} latest={latest_timestamp.isoformat()} path={self.path}"
        )


class DatastoreApiClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_seconds: int = 30,
        request_delay_seconds: float = 0.0,
    ) -> None:
        normalized_base = base_url.rstrip("/") + "/"
        self.bulk_url = urljoin(normalized_base, "messages/bulk")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.request_delay_seconds = request_delay_seconds
        self._next_request_monotonic = 0.0

    async def wait_for_rate_limit(self) -> None:
        if self.request_delay_seconds <= 0:
            return

        now = time.monotonic()
        if now < self._next_request_monotonic:
            wait_seconds = self._next_request_monotonic - now
            log(f"Throttling API request for {wait_seconds:.2f}s")
            await asyncio.sleep(wait_seconds)

        self._next_request_monotonic = time.monotonic() + self.request_delay_seconds

    async def post_messages_bulk(self, messages: list[dict[str, str]]) -> int:
        if not messages:
            return 0

        await self.wait_for_rate_limit()
        payload = {"messages": messages}
        return await asyncio.to_thread(self._post_messages_bulk_blocking, payload)

    def _post_messages_bulk_blocking(self, payload: dict[str, Any]) -> int:
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
                    log(
                        f"API bulk write success status={status} requested={len(payload['messages'])} inserted={inserted}"
                    )
                    if isinstance(inserted, int):
                        return inserted
                    return len(payload["messages"])

                raise RuntimeError(f"unexpected status code: {status}")
            except HTTPError as error:
                status = error.code
                detail = error.read().decode("utf-8", "replace")
                retriable = status >= 500 or status == 429
                if not retriable or attempt == max_attempts:
                    raise RuntimeError(
                        f"bulk POST failed with status {status}: {detail}"
                    ) from error
                log(
                    f"API bulk write retry attempt={attempt}/{max_attempts} status={status} backoff={backoff_seconds:.2f}s"
                )
            except URLError as error:
                if attempt == max_attempts:
                    raise RuntimeError(
                        f"bulk POST failed after retries: {error}"
                    ) from error
                log(
                    f"API bulk write retry attempt={attempt}/{max_attempts} reason=network-error backoff={backoff_seconds:.2f}s"
                )
            except TimeoutError as error:
                if attempt == max_attempts:
                    raise RuntimeError("bulk POST timed out after retries") from error
                log(
                    f"API bulk write retry attempt={attempt}/{max_attempts} reason=timeout backoff={backoff_seconds:.2f}s"
                )

            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 8.0)

        raise RuntimeError("bulk POST failed after retries")


class DiscordSelfScraper(discord.Client):
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        self.state_store = StateStore(config.state_file)
        self.api_client = DatastoreApiClient(
            config.api_base_url,
            config.api_key,
            request_delay_seconds=config.request_delay_seconds,
        )
        self.total_seen = 0
        self.total_inserted = 0
        self.failed_channels = 0

    async def on_ready(self) -> None:
        log(f"Logged in as {self.user}")
        log(
            f"Run config channel_ids={len(self.config.channel_ids)} dm_only={self.config.dm_only} list_channels={self.config.list_channels} limit={self.config.limit} batch_size={self.config.batch_size} request_delay_seconds={self.config.request_delay_seconds} shard_index={self.config.shard_index} shard_count={self.config.shard_count}"
        )
        try:
            if self.config.list_channels:
                await self.list_accessible_channels()

            channel_ids = self.resolve_target_channel_ids()
            if not channel_ids:
                log("No target channels selected. Exiting.")
                return

            for channel_id in channel_ids:
                try:
                    await self.scrape_channel(channel_id)
                except Exception as error:
                    self.failed_channels += 1
                    log(f"Channel {channel_id} failed: {error}")

            log(
                "Finished scrape "
                f"(seen={self.total_seen}, inserted={self.total_inserted}, channels={len(channel_ids)}, failed={self.failed_channels})"
            )
        finally:
            await self.close()

    def resolve_target_channel_ids(self) -> list[int]:
        if self.config.dm_only:
            dm_channel_ids = self.get_dm_channel_ids()
            if self.config.dm_max_channels is not None:
                dm_channel_ids = dm_channel_ids[: self.config.dm_max_channels]
            selected_channel_ids = self.apply_shard(dm_channel_ids)
            log(
                f"DM-only mode selected {len(selected_channel_ids)}/{len(dm_channel_ids)} channel(s)"
            )
            return selected_channel_ids

        return self.apply_shard(self.config.channel_ids)

    def apply_shard(self, channel_ids: list[int]) -> list[int]:
        if self.config.shard_index is None or self.config.shard_count is None:
            return channel_ids

        selected = [
            channel_id
            for channel_id in channel_ids
            if channel_id % self.config.shard_count == self.config.shard_index
        ]
        log(
            f"Shard filter selected {len(selected)}/{len(channel_ids)} channel(s) for shard {self.config.shard_index + 1}/{self.config.shard_count}"
        )
        return selected

    def get_dm_channel_ids(self) -> list[int]:
        private_channels = getattr(self, "private_channels", [])
        dm_channel_ids: list[int] = []

        for channel in private_channels:
            if hasattr(channel, "history"):
                dm_channel_ids.append(channel.id)

        return dm_channel_ids

    async def list_accessible_channels(self) -> None:
        log("Visible guild channels:")
        for guild in self.guilds:
            me = guild.get_member(self.user.id) if self.user else None
            for channel in guild.text_channels:
                can_view = True
                if me is not None:
                    can_view = channel.permissions_for(me).view_channel

                if not can_view:
                    continue

                log(
                    f"- guild={guild.name} channel={channel.name} channel_id={channel.id}"
                )

        private_channels = getattr(self, "private_channels", [])
        if private_channels:
            log("Visible private channels:")
            for channel in private_channels:
                channel_name = getattr(channel, "name", None) or str(channel)
                log(f"- channel={channel_name} channel_id={channel.id}")

    async def scrape_channel(self, channel_id: int) -> None:
        log(f"Scraping channel {channel_id}...")

        channel = self.get_channel(channel_id)
        if channel is None:
            channel = await self.fetch_channel(channel_id)

        if not hasattr(channel, "history"):
            print(f"Skipping {channel_id}: channel type has no history()")
            return

        effective_after = self.state_store.get_after(channel_id, self.config.after)
        if effective_after:
            log(f"Channel {channel_id} using after={effective_after.isoformat()}")

        buffered: list[tuple[dict[str, str], datetime]] = []
        inserted_for_channel = 0
        seen_for_channel = 0

        async for message in channel.history(
            limit=self.config.limit,
            before=self.config.before,
            after=effective_after,
            oldest_first=True,
        ):
            seen_for_channel += 1
            self.total_seen += 1

            if seen_for_channel % 100 == 0:
                log(
                    f"Progress channel={channel_id} seen={seen_for_channel} buffered={len(buffered)} inserted_total={self.total_inserted}"
                )

            payload = self.message_to_payload(message)
            if payload is None:
                continue

            buffered.append((payload, message.created_at))
            if len(buffered) >= self.config.batch_size:
                inserted = await self.flush_batch(channel_id, buffered)
                inserted_for_channel += inserted
                self.total_inserted += inserted
                log(
                    f"Posted batch channel={channel_id} size={len(buffered)} inserted={inserted}"
                )
                buffered = []

        if buffered:
            inserted = await self.flush_batch(channel_id, buffered)
            inserted_for_channel += inserted
            self.total_inserted += inserted
            log(
                f"Posted final batch channel={channel_id} size={len(buffered)} inserted={inserted}"
            )

        log(
            f"Done channel={channel_id} seen={seen_for_channel} inserted={inserted_for_channel}"
        )

    def message_to_payload(self, message: discord.Message) -> dict[str, str] | None:
        if not self.config.include_bots and message.author.bot:
            return None

        parts: list[str] = []
        content = message.content.strip()
        if content:
            parts.append(content)

        for attachment in message.attachments:
            parts.append(f"[attachment] {attachment.url}")

        if not parts:
            return None

        return {
            "user": f"{message.author.name}:{message.author.id}",
            "thread": self.resolve_message_thread(message),
            "platform_uuid": str(message.id),
            "platform": self.config.platform,
            "message": "\n".join(parts),
        }

    def resolve_message_thread(self, message: discord.Message) -> str:
        channel = message.channel

        recipient = getattr(channel, "recipient", None)
        if recipient is not None:
            return f"to:{recipient.name}:{recipient.id}"

        recipients = getattr(channel, "recipients", None)
        if recipients:
            recipient_ids = ",".join(str(user.id) for user in recipients)
            return f"to_group:{recipient_ids}"

        channel_id = getattr(channel, "id", None)
        if channel_id is not None:
            return f"channel:{channel_id}"

        return "channel:unknown"

    async def flush_batch(
        self, channel_id: int, batch: list[tuple[dict[str, str], datetime]]
    ) -> int:
        messages = [payload for payload, _ in batch]
        inserted = await self.api_client.post_messages_bulk(messages)
        latest_timestamp = max(created_at for _, created_at in batch)
        self.state_store.update(channel_id, latest_timestamp)
        return inserted


def build_config() -> Config:
    parser = argparse.ArgumentParser(
        description="Scrape Discord messages and POST to datastore API"
    )
    parser.add_argument(
        "--channel-id", type=int, action="append", help="Channel ID (repeatable)"
    )
    parser.add_argument(
        "--limit", type=int, default=500, help="Messages per channel to fetch"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Messages per API bulk request"
    )
    parser.add_argument(
        "--include-bots", action="store_true", help="Include bot-authored messages"
    )
    parser.add_argument("--before", type=str, help="Only messages before ISO datetime")
    parser.add_argument("--after", type=str, help="Only messages after ISO datetime")
    parser.add_argument(
        "--platform",
        type=str,
        default="DISCORD_SELF",
        help="Platform label stored in datastore",
    )
    parser.add_argument(
        "--list-channels",
        action="store_true",
        help="List visible channels before scraping",
    )
    parser.add_argument(
        "--dm-only",
        action="store_true",
        help="Ignore --channel-id values and scrape all visible DM/group DM channels",
    )
    parser.add_argument(
        "--dm-max-channels",
        type=int,
        help="When using --dm-only, limit how many DM/group DM channels to scrape",
    )
    parser.add_argument(
        "--request-delay-seconds",
        type=float,
        default=0.0,
        help="Minimum delay between outbound API write requests",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        help="0-based shard index when distributing channels across workers",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        help="Total number of shards/workers when distributing channels",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default=".state/discord_self.json",
        help="Path to state file used for incremental scraping (set empty string to disable)",
    )
    args = parser.parse_args()

    if args.limit <= 0:
        raise ValueError("--limit must be positive")

    if args.batch_size <= 0 or args.batch_size > 500:
        raise ValueError("--batch-size must be between 1 and 500")

    if args.dm_max_channels is not None and args.dm_max_channels <= 0:
        raise ValueError("--dm-max-channels must be positive")

    if args.request_delay_seconds < 0:
        raise ValueError("--request-delay-seconds cannot be negative")

    if (args.shard_index is None) != (args.shard_count is None):
        raise ValueError("--shard-index and --shard-count must be set together")

    if args.shard_count is not None and args.shard_count <= 0:
        raise ValueError("--shard-count must be positive")

    if args.shard_index is not None and args.shard_index < 0:
        raise ValueError("--shard-index must be >= 0")

    if (
        args.shard_index is not None
        and args.shard_count is not None
        and args.shard_index >= args.shard_count
    ):
        raise ValueError("--shard-index must be less than --shard-count")

    if not args.list_channels and not args.dm_only and not args.channel_id:
        raise ValueError(
            "Provide --channel-id, or pass --dm-only, or pass --list-channels"
        )

    load_dotenv()

    token = os.getenv("DISCORD_USER_TOKEN")
    api_base_url = os.getenv("DATASTORE_API_BASE_URL")
    api_key = os.getenv("DATASTORE_API_KEY")

    if not token:
        raise ValueError("DISCORD_USER_TOKEN is required")

    if not api_base_url:
        raise ValueError("DATASTORE_API_BASE_URL is required")

    if not api_key:
        raise ValueError("DATASTORE_API_KEY is required")

    return Config(
        token=token,
        api_base_url=api_base_url,
        api_key=api_key,
        channel_ids=args.channel_id or [],
        limit=args.limit,
        batch_size=args.batch_size,
        include_bots=args.include_bots,
        before=parse_datetime(args.before),
        after=parse_datetime(args.after),
        platform=args.platform,
        state_file=args.state_file if args.state_file else None,
        list_channels=args.list_channels,
        dm_only=args.dm_only,
        dm_max_channels=args.dm_max_channels,
        request_delay_seconds=args.request_delay_seconds,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
    )


async def main() -> None:
    config = build_config()
    client = DiscordSelfScraper(config)
    await client.start(config.token)


if __name__ == "__main__":
    asyncio.run(main())
