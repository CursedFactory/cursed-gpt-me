# imessage_csv scraper

Imports iMessage/SMS GDPR CSV exports and POSTs rows to the local datastore HTTP API.

## Prerequisites

- Python 3.10+
- Local datastore server running (see `datastore/README.md`)
- `DATASTORE_API_KEY` configured

## Setup

```bash
cd scrapers/imessage_csv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill `.env`:

- `DATASTORE_API_BASE_URL`: usually `http://127.0.0.1:4040`
- `DATASTORE_API_KEY`: same key used by `datastore-server`

## Run

```bash
python scrape.py
```

Default behavior:

- Auto-discovers the first CSV in `~/data/vfp_gdpr/imessage`
- Imports in batches of 200
- Stores with platform `IMESSAGE`
- Uses `Contact` as `thread` (falls back to `Sender`)
- Generates deterministic `platform_uuid` from CSV row content for duplicate checks

Optional flags:

- `--csv-path ~/data/vfp_gdpr/imessage/imessages_export.csv`
- `--batch-size 100`
- `--limit 5000`
- `--platform IMESSAGE`

## Data sent to API

`POST /messages/bulk` payload shape:

```json
{
  "messages": [
    {
      "user": "Me:Unknown",
      "thread": "Unknown",
      "platform_uuid": "f6cbb41f-3b8e-5e57-ac36-9734d3f61c0f",
      "platform": "IMESSAGE",
      "message": "[2021-03-01 02:30:56][SMS] where'd ya go"
    }
  ]
}
```
