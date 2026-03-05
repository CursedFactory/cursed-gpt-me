# datastore

Bun workspace containing:

- `datastore-core`: Prisma + SQLite library for storing raw `text_messages` rows.
- `datastore-cli`: CLI for seeding, querying, and debugging datastore-core.
- `datastore-server`: Local Elysia HTTP API for scraper ingestion and querying.

## Quickstart

```bash
cd datastore
bun install
bun run core:migrate
bun run cli -- seed
bun run cli -- stats
bun run cli -- metrics
```

## Local HTTP API

```bash
cd datastore
export DATASTORE_API_KEY="replace-me"
bun run server
```

Default listen address: `127.0.0.1:4040`.

Optional overrides:

- `DATASTORE_SERVER_HOST`
- `DATASTORE_SERVER_PORT`

### Endpoints

- `GET /health`
- `GET /stats`
- `GET /metrics`
- `GET /messages?limit=50&platform=DISCORD_SELF&user=alice&thread=channel:1234&platform_uuid=1234567890`
- `POST /messages` (requires header `X-API-Key`)
- `POST /messages/bulk` (requires header `X-API-Key`)

### Example bulk insert

```bash
curl -sS http://127.0.0.1:4040/messages/bulk \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $DATASTORE_API_KEY" \
  -d '{"messages":[{"user":"alice","thread":"channel:1234","platform_uuid":"1234567890","platform":"DISCORD_SELF","message":"hello"}]}'
```
