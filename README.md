# cursed-gpt-me
Local LLMs GPT2 based on GPDR data. 

## Components

- `datastore/`: Bun workspace with datastore core library, CLI, and local HTTP server.
- `scrapers/discord_self/`: Python Discord self scraper that POSTs messages to local datastore API.
- `scrapers/imessage_csv/`: Python CSV importer for iMessage/SMS GDPR exports into datastore API.
