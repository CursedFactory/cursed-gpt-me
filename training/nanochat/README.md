# nanochat Training from SQLite

This workspace contains a single pipeline script that prepares nanochat pretraining shards from the local SQLite datastore and optionally runs tokenizer + base model training.

Script:

- `training/nanochat/train_nanochat_from_sqlite.py`

What it does:

1. Reads messages from `text_messages` in SQLite.
2. Groups messages by thread into sliding-window documents.
3. Writes nanochat-compatible parquet shards (`text` column) to `NANOCHAT_BASE_DIR/base_data_climbmix`.
4. Runs `python -m scripts.tok_train` in `approaches/nanochat`.
5. Runs `python -m scripts.base_train` in `approaches/nanochat`.

## Setup

Install nanochat dependencies first (from `approaches/nanochat`):

```bash
uv venv
uv sync --extra gpu
source .venv/bin/activate
```

If you are CPU-only, use `uv sync --extra cpu`.

Install Aim for experiment tracking:

```bash
uv add aim
```

## Usage

From repo root:

```bash
python training/nanochat/train_nanochat_from_sqlite.py \
  --tracker=aim \
  --device-type=cuda \
  --run=sqlite-nanochat \
  --depth=12 \
  --max-seq-len=512 \
  --device-batch-size=8 \
  --total-batch-size=32768 \
  --num-iterations=3000
```

Useful flags:

- `--db-path /path/to/datastore.db`
- `--nanochat-base-dir training/nanochat/artifacts/nanochat_cache`
- `--plain-text` (use `[USER]: MESSAGE` lines instead of metadata tags)
- `--tracker aim|wandb|none`
- `--run auto` (default; generates timestamped run names)
- `--label simplerprompt` (when `--run=auto`, use this exact run name)
- `--validation-ratio 0.1`
- `--messages-per-sample 16`
- `--sample-overlap 4`
- `--skip-tokenizer` (reuse existing tokenizer)
- `--skip-training` (export parquet only)
- `--sample-every 50` (logs sample generations to tracker)
- `--base-train-arg=--warmup-ratio=0.03` (pass-through to `scripts.base_train`)

## Outputs

By default, outputs are written under:

- `training/nanochat/artifacts/nanochat_cache/`

Including:

- `base_data_climbmix/shard_*.parquet` (training + validation shards)
- `tokenizer/` (tokenizer artifacts)
- `base_checkpoints/` (model checkpoints)
