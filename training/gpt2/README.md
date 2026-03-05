# GPT-2 Training Workspace

This workspace contains the first-pass GPT-2 training pipeline for data stored in the repo's SQLite datastore.

## Scope

- Export from SQLite (`text_messages`) to JSONL training samples
- Fine-tune a GPT-2 style causal LM (`distilgpt2` default)
- Save checkpoints + run summary

## Setup

```bash
cd training/gpt2
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 1) Export SQLite to JSONL

Default DB input path:

- `data/datastore.db`

Default output path:

- `training/gpt2/data/processed/messages.jsonl`

Run:

```bash
python export_sqlite_to_jsonl.py
```

Useful flags:

- `--db-path /path/to/datastore.db`
- `--output-path training/gpt2/data/processed/messages.jsonl`
- `--messages-per-sample 16`
- `--sample-overlap 4`
- `--min-messages-per-sample 2`
- `--plain-text` (omit metadata tags)

## 2) Fine-tune GPT-2

Run with defaults:

```bash
python train_gpt2.py
```

Defaults include:

- model: `distilgpt2`
- block size: `256`
- epochs: `3`
- validation split: `2%` (time-ordered split; no shuffle)

Useful flags:

- `--model-name openai-community/gpt2`
- `--train-file training/gpt2/data/processed/messages.jsonl`
- `--validation-file training/gpt2/data/processed/messages_val.jsonl`
- `--output-dir training/gpt2/artifacts/runs/manual-run`
- `--per-device-train-batch-size 2`
- `--gradient-accumulation-steps 16`
- `--fp16` or `--bf16`

## Outputs

Each run writes to:

- `training/gpt2/artifacts/runs/<timestamp>/`

Including:

- model checkpoints,
- tokenizer files,
- `run_summary.json`.
