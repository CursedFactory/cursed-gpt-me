# discord_self scraper

Scrapes messages from selected Discord channels using `discord.py-self` and POSTs them to the local datastore HTTP API.

## Warning

Automating user accounts may violate Discord Terms of Service.
Use this only for your own authorized data collection and at your own risk.

## Prerequisites

- Python 3.10+
- Local datastore server running (see `datastore/README.md`)
- `DATASTORE_API_KEY` configured

## Setup

```bash
cd scrapers/discord_self
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill `.env`:

- `DISCORD_USER_TOKEN`: your own authorized token you already possess.
- `DATASTORE_API_BASE_URL`: usually `http://127.0.0.1:4040`.
- `DATASTORE_API_KEY`: same key used by `datastore-server`.

## Run

```bash
python scrape.py --channel-id 123456789012345678 --channel-id 234567890123456789 --limit 1000 --batch-size 50
```

By default the scraper writes progress checkpoints to `.state/discord_self.json` and resumes incrementally per channel on the next run.

Optional filters:

- `--before 2026-01-01T00:00:00+00:00`
- `--after 2025-12-01T00:00:00+00:00`
- `--include-bots`
- `--platform DISCORD_SELF`
- `--state-file .state/discord_self.json` (set `--state-file ""` to disable checkpoints)
- `--list-channels` (print visible guild + DM channels)
- `--dm-only` (scan visible DMs/group DMs and scrape those, no explicit channel IDs needed)
- `--dm-max-channels 10` (with `--dm-only`, scrape only first N DM/group DM channels)
- `--request-delay-seconds 0.5` (minimum delay between API write requests)
- `--shard-index 0 --shard-count 4` (split channels deterministically across multiple workers)

### Parallel sharded runs

You can run multiple scraper processes in parallel and split target channels across them.
Sharding works with both explicit `--channel-id` lists and `--dm-only` mode.

Example with 4 workers (run each in a separate shell):

```bash
python scrape.py --dm-only --limit 100000 --request-delay-seconds 0.2 --shard-index 0 --shard-count 4 --state-file .state/discord_self_shard_0.json
python scrape.py --dm-only --limit 100000 --request-delay-seconds 0.2 --shard-index 1 --shard-count 4 --state-file .state/discord_self_shard_1.json
python scrape.py --dm-only --limit 100000 --request-delay-seconds 0.2 --shard-index 2 --shard-count 4 --state-file .state/discord_self_shard_2.json
python scrape.py --dm-only --limit 100000 --request-delay-seconds 0.2 --shard-index 3 --shard-count 4 --state-file .state/discord_self_shard_3.json
```

## Data sent to API

`POST /messages/bulk` payload shape:

```json
{
  "messages": [
    {
      "user": "alice:1234567890",
      "thread": "to:bob:9988776655",
      "platform_uuid": "1459787427733909504",
      "platform": "DISCORD_SELF",
      "message": "Hello from Discord"
    }
  ]
}
```
