import random
from typing import Dict, List, Tuple

from aim.hugging_face import AimCallback
from datasets import Dataset
from transformers import TrainerCallback
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    DataCollatorForLanguageModeling,
    Trainer,
    TrainingArguments,
)

from src.discord_datastore import Datastore


def extract_contents_from_db() -> List[str]:
    with Datastore() as datastore:
        rows = datastore.connection.execute(
            """
            WITH unique_messages AS (
                SELECT id, timestamps, content, username, platform
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY timestamps, content, username, platform
                               ORDER BY id
                           ) AS row_num
                    FROM messages
                )
                WHERE row_num = 1
            )
            SELECT content FROM unique_messages ORDER BY timestamps ASC, id ASC
            """
        ).fetchall()

    messages: List[str] = []
    for row in rows:
        text = (row[0] or "").strip()
        if text:
            messages.append(text)
    return messages


def build_examples(messages: List[str], window: int = 8):
    for i in range(window, len(messages)):
        context = messages[i - window : i]
        target = messages[i]

        text = (
            "<|context|>\n"
            + "\n".join(context)
            + "\n<|next|>\n"
            + target
            + "\n<|end|>\n"
            + "<|eot|>\n"
        )
        yield {"text": text}


def make_splits(messages: List[str], window: int = 8, valid_frac: float = 0.05, seed: int = 42) -> Tuple[Dataset, Dataset]:
    examples = list(build_examples(messages, window=window))
    if not examples:
        raise RuntimeError("No training examples were created. Add more messages or lower the context window.")

    random.Random(seed).shuffle(examples)

    if len(examples) < 2:
        return Dataset.from_list(examples), Dataset.from_list(examples)

    cut = int((1.0 - valid_frac) * len(examples))
    cut = min(max(cut, 1), len(examples) - 1)
    train_list = examples[:cut]
    valid_list = examples[cut:]

    return Dataset.from_list(train_list), Dataset.from_list(valid_list)


def tokenize_and_chunk(train_ds: Dataset, valid_ds: Dataset, tok: AutoTokenizer, block_size: int = 256):
    def tokenize(batch: Dict[str, List[str]]):
        return tok(
            batch["text"],
            truncation=True,
            max_length=block_size,
            padding="max_length",
        )

    train_tok = train_ds.map(tokenize, batched=True, remove_columns=["text"])
    valid_tok = valid_ds.map(tokenize, batched=True, remove_columns=["text"])

    def add_labels(batch: Dict[str, List[List[int]]]):
        return {"labels": [ids[:] for ids in batch["input_ids"]]}

    train_lm = train_tok.map(add_labels, batched=True)
    valid_lm = valid_tok.map(add_labels, batched=True)
    return train_lm, valid_lm


def print_validation_examples(valid_ds: Dataset, n: int = 3) -> None:
    count = min(n, len(valid_ds))
    if count == 0:
        return

    print("\nValidation text examples:")
    for i in range(count):
        print(f"\n--- valid example {i + 1} ---")
        print(valid_ds[i]["text"])


def build_generation_prompts(valid_ds: Dataset, n: int = 2) -> List[str]:
    prompts: List[str] = []
    count = min(n, len(valid_ds))
    for i in range(count):
        text = valid_ds[i]["text"]
        marker = "\n<|next|>\n"
        idx = text.find(marker)
        if idx == -1:
            continue
        prompts.append(text[: idx + len(marker)])
    return prompts


class SampleEvalCallback(TrainerCallback):
    def __init__(self, prompts: List[str], max_new_tokens: int = 40):
        self.prompts = prompts
        self.max_new_tokens = max_new_tokens

    def on_evaluate(self, args, state, control, **kwargs):
        model = kwargs.get("model")
        tokenizer = kwargs.get("processing_class") or kwargs.get("tokenizer")
        if model is None or tokenizer is None or not self.prompts:
            return

        print("\nSample generations (current model):")
        for i, prompt in enumerate(self.prompts, 1):
            inputs = tokenizer(prompt, return_tensors="pt")
            inputs = {k: v.to(model.device) for k, v in inputs.items()}
            output_ids = model.generate(
                **inputs,
                max_new_tokens=self.max_new_tokens,
                do_sample=True,
                top_p=0.9,
                temperature=0.8,
                pad_token_id=tokenizer.pad_token_id,
            )
            generated = tokenizer.decode(output_ids[0], skip_special_tokens=False)
            print(f"\n--- eval sample {i} ---")
            print(generated)


def main() -> None:
    messages = extract_contents_from_db()
    if len(messages) < 2:
        raise RuntimeError("Need at least 2 messages in datastore to train")

    window = min(8, len(messages) - 1)
    train_ds, valid_ds = make_splits(messages, window=window, valid_frac=0.05, seed=42)
    print_validation_examples(valid_ds, n=3)
    sample_prompts = build_generation_prompts(valid_ds, n=2)

    model_name = "gpt2"
    tok = AutoTokenizer.from_pretrained(model_name)
    tok.add_special_tokens(
        {
            "pad_token": "<|pad|>",
            "additional_special_tokens": ["<|context|>", "<|next|>", "<|end|>", "<|eot|>"],
        }
    )
    tok.padding_side = "right"

    train_lm, valid_lm = tokenize_and_chunk(train_ds, valid_ds, tok, block_size=64)

    model = AutoModelForCausalLM.from_pretrained(model_name)
    model.resize_token_embeddings(len(tok), mean_resizing=False)
    model.config.pad_token_id = tok.pad_token_id
    model.generation_config.pad_token_id = tok.pad_token_id

    collator = DataCollatorForLanguageModeling(tokenizer=tok, mlm=False)
    aim_cb = AimCallback(repo="aim_logs", experiment="gpt2-next-message")
    sample_cb = SampleEvalCallback(sample_prompts)

    args = TrainingArguments(
        output_dir="out_gpt2_chat",
        per_device_train_batch_size=2,
        per_device_eval_batch_size=2,
        gradient_accumulation_steps=8,
        learning_rate=5e-5,
        num_train_epochs=1,
        eval_strategy="steps",
        eval_steps=50,
        logging_steps=50,
        save_steps=50,
        save_total_limit=2,
        dataloader_pin_memory=False,
        report_to=[],
    )

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=train_lm,
        eval_dataset=valid_lm,
        processing_class=tok,
        data_collator=collator,
        callbacks=[aim_cb, sample_cb],
    )

    trainer.train()
    trainer.save_model("out_gpt2_chat/final")
    tok.save_pretrained("out_gpt2_chat/final")


if __name__ == "__main__":
    main()
