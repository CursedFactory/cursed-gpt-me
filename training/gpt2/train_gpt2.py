import argparse
import json
import math
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any

from datasets import DatasetDict, load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    DataCollatorForLanguageModeling,
    Trainer,
    TrainingArguments,
    set_seed,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRAIN_FILE = (
    REPO_ROOT / "training" / "gpt2" / "data" / "processed" / "messages.jsonl"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "training" / "gpt2" / "artifacts" / "runs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fine-tune a GPT-2 model on exported JSONL conversation data",
    )
    parser.add_argument(
        "--train-file",
        type=str,
        default=str(DEFAULT_TRAIN_FILE),
        help="Path to training JSONL file containing a `text` column",
    )
    parser.add_argument(
        "--validation-file",
        type=str,
        default=None,
        help="Optional validation JSONL file containing a `text` column",
    )
    parser.add_argument(
        "--validation-ratio",
        type=float,
        default=0.02,
        help="Validation ratio when --validation-file is not provided",
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default="distilgpt2",
        help="Base model checkpoint to fine-tune",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for checkpoints and metrics",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=256,
        help="Token chunk size used for causal LM training",
    )
    parser.add_argument("--num-train-epochs", type=float, default=3.0)
    parser.add_argument("--learning-rate", type=float, default=2e-5)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--warmup-ratio", type=float, default=0.03)
    parser.add_argument("--per-device-train-batch-size", type=int, default=2)
    parser.add_argument("--per-device-eval-batch-size", type=int, default=2)
    parser.add_argument("--gradient-accumulation-steps", type=int, default=16)
    parser.add_argument("--logging-steps", type=int, default=50)
    parser.add_argument("--save-total-limit", type=int, default=2)
    parser.add_argument("--max-train-samples", type=int, default=0)
    parser.add_argument("--max-validation-samples", type=int, default=0)
    parser.add_argument("--num-proc", type=int, default=0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--fp16", action="store_true", help="Enable fp16 training")
    parser.add_argument("--bf16", action="store_true", help="Enable bf16 training")

    args = parser.parse_args()

    if args.validation_file is None and not (0 < args.validation_ratio < 1):
        raise ValueError("--validation-ratio must be between 0 and 1")

    if args.block_size <= 0:
        raise ValueError("--block-size must be > 0")

    if args.gradient_accumulation_steps <= 0:
        raise ValueError("--gradient-accumulation-steps must be > 0")

    if args.max_train_samples < 0 or args.max_validation_samples < 0:
        raise ValueError(
            "--max-train-samples and --max-validation-samples must be >= 0"
        )

    if args.num_proc < 0:
        raise ValueError("--num-proc must be >= 0")

    if args.fp16 and args.bf16:
        raise ValueError("Choose only one of --fp16 or --bf16")

    return args


def resolve_output_dir(raw_output_dir: str | None) -> Path:
    if raw_output_dir:
        return Path(raw_output_dir).expanduser().resolve()

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return (DEFAULT_OUTPUT_ROOT / timestamp).resolve()


def load_raw_dataset(args: argparse.Namespace) -> DatasetDict:
    train_file = Path(args.train_file).expanduser().resolve()
    if not train_file.exists():
        raise ValueError(f"Training file not found: {train_file}")

    if args.validation_file:
        validation_file = Path(args.validation_file).expanduser().resolve()
        if not validation_file.exists():
            raise ValueError(f"Validation file not found: {validation_file}")

        raw_dataset = load_dataset(
            "json",
            data_files={
                "train": str(train_file),
                "validation": str(validation_file),
            },
        )
        return DatasetDict(
            {
                "train": raw_dataset["train"],
                "validation": raw_dataset["validation"],
            },
        )

    only_train = load_dataset("json", data_files={"train": str(train_file)})["train"]
    split = only_train.train_test_split(test_size=args.validation_ratio, shuffle=False)
    return DatasetDict({"train": split["train"], "validation": split["test"]})


def apply_sample_limits(dataset: DatasetDict, args: argparse.Namespace) -> DatasetDict:
    train_split = dataset["train"]
    validation_split = dataset["validation"]

    if args.max_train_samples > 0:
        train_size = min(len(train_split), args.max_train_samples)
        train_split = train_split.select(range(train_size))

    if args.max_validation_samples > 0:
        validation_size = min(len(validation_split), args.max_validation_samples)
        validation_split = validation_split.select(range(validation_size))

    return DatasetDict({"train": train_split, "validation": validation_split})


def build_tokenized_dataset(
    dataset: DatasetDict,
    tokenizer: Any,
    block_size: int,
    num_proc: int,
) -> DatasetDict:
    if "text" not in dataset["train"].column_names:
        raise ValueError("Dataset must include a `text` column")

    remove_columns = dataset["train"].column_names
    map_num_proc = num_proc if num_proc > 1 else None

    def tokenize_function(batch: dict[str, list[str]]) -> dict[str, Any]:
        return tokenizer(batch["text"])

    tokenized = dataset.map(
        tokenize_function,
        batched=True,
        num_proc=map_num_proc,
        remove_columns=remove_columns,
        desc="Tokenizing text",
    )

    def group_texts(examples: dict[str, list[list[int]]]) -> dict[str, list[list[int]]]:
        concatenated = {
            key: list(chain.from_iterable(examples[key])) for key in examples.keys()
        }
        total_length = len(concatenated["input_ids"])
        total_length = (total_length // block_size) * block_size
        if total_length == 0:
            return {"input_ids": [], "attention_mask": [], "labels": []}

        grouped = {
            key: [
                tokens[i : i + block_size] for i in range(0, total_length, block_size)
            ]
            for key, tokens in concatenated.items()
        }
        grouped["labels"] = grouped["input_ids"].copy()
        return grouped

    lm_dataset = tokenized.map(
        group_texts,
        batched=True,
        num_proc=map_num_proc,
        desc=f"Grouping tokens into {block_size}-token chunks",
    )

    if len(lm_dataset["train"]) == 0:
        raise ValueError(
            "No train chunks were created. Increase dataset size or reduce --block-size.",
        )

    if len(lm_dataset["validation"]) == 0:
        raise ValueError(
            "No validation chunks were created. Increase dataset size or reduce --block-size.",
        )

    return lm_dataset


def safe_perplexity(eval_loss: float | None) -> float | None:
    if eval_loss is None:
        return None

    try:
        return float(math.exp(eval_loss))
    except OverflowError:
        return None


def main() -> None:
    args = parse_args()
    output_dir = resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    set_seed(args.seed)

    raw_dataset = load_raw_dataset(args)
    raw_dataset = apply_sample_limits(raw_dataset, args)

    tokenizer = AutoTokenizer.from_pretrained(args.model_name)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model_max_length = tokenizer.model_max_length
    if model_max_length is None or model_max_length > 100_000:
        model_max_length = 1024

    block_size = min(args.block_size, int(model_max_length))
    if block_size != args.block_size:
        print(
            f"Requested block size {args.block_size} exceeds model max length {model_max_length}; using {block_size}",
        )

    lm_dataset = build_tokenized_dataset(
        dataset=raw_dataset,
        tokenizer=tokenizer,
        block_size=block_size,
        num_proc=args.num_proc,
    )

    model = AutoModelForCausalLM.from_pretrained(args.model_name)
    data_collator = DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False)

    training_kwargs = {
        "output_dir": str(output_dir),
        "overwrite_output_dir": True,
        "learning_rate": args.learning_rate,
        "weight_decay": args.weight_decay,
        "num_train_epochs": args.num_train_epochs,
        "per_device_train_batch_size": args.per_device_train_batch_size,
        "per_device_eval_batch_size": args.per_device_eval_batch_size,
        "gradient_accumulation_steps": args.gradient_accumulation_steps,
        "warmup_ratio": args.warmup_ratio,
        "logging_steps": args.logging_steps,
        "save_strategy": "epoch",
        "save_total_limit": args.save_total_limit,
        "load_best_model_at_end": True,
        "metric_for_best_model": "eval_loss",
        "greater_is_better": False,
        "report_to": "none",
        "seed": args.seed,
        "fp16": args.fp16,
        "bf16": args.bf16,
    }

    try:
        training_args = TrainingArguments(
            evaluation_strategy="epoch",
            **training_kwargs,
        )
    except TypeError:
        training_args = TrainingArguments(
            eval_strategy="epoch",
            **training_kwargs,
        )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=lm_dataset["train"],
        eval_dataset=lm_dataset["validation"],
        data_collator=data_collator,
    )

    train_result = trainer.train()
    eval_result = trainer.evaluate()
    perplexity = safe_perplexity(eval_result.get("eval_loss"))

    trainer.save_model(str(output_dir))
    tokenizer.save_pretrained(str(output_dir))
    trainer.save_state()

    summary = {
        "model_name": args.model_name,
        "block_size": block_size,
        "train_examples": len(raw_dataset["train"]),
        "validation_examples": len(raw_dataset["validation"]),
        "train_chunks": len(lm_dataset["train"]),
        "validation_chunks": len(lm_dataset["validation"]),
        "train_runtime_seconds": train_result.metrics.get("train_runtime"),
        "train_loss": train_result.metrics.get("train_loss"),
        "eval_loss": eval_result.get("eval_loss"),
        "perplexity": perplexity,
        "output_dir": str(output_dir),
    }

    summary_path = output_dir / "run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Training complete: {output_dir}")
    print(f"Train chunks: {len(lm_dataset['train'])}")
    print(f"Validation chunks: {len(lm_dataset['validation'])}")
    print(f"Eval loss: {summary['eval_loss']}")
    print(f"Perplexity: {summary['perplexity']}")
    print(f"Summary file: {summary_path}")


if __name__ == "__main__":
    main()
