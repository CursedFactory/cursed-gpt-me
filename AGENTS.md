# AGENTS.md

## Project Overview
This is a demo project for training GPT2 on Discord messages with monitoring using AIM. The project demonstrates:
- Discord message data import and processing
- GPT2 model training
- AIM (Arize AI Monitoring) integration for model monitoring

## Build, Lint, and Test Commands

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Build
```bash
# Build project (if applicable)
python setup.py build
```

### Lint
```bash
# Run linters
ruff check .
# or
flake8 .
# or
black --check .
```

### Format
```bash
# Format code
black .
ruff format .
```

### Type Check
```bash
# Type checking
mypy .
```

### Run Tests
```bash
# Run all tests
pytest

# Run single test
pytest tests/test_specific_module.py::test_function_name

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_gpt2_training.py

# Run test with verbose output
pytest -v tests/test_specific_module.py::test_function_name

# Run tests matching a pattern
pytest -k "test_gpt2"

# Run tests in a specific directory
pytest tests/gpt2/
```

## Code Style Guidelines

### Python Code Style
- Follow PEP 8 style guide
- Use `black` for code formatting (110 character line limit)
- Use `ruff` for linting and import sorting
- Use `mypy` for type checking
- Use `isort` for import organization

### Import Organization
```python
# Standard library imports first
import os
import sys
from typing import List, Optional

# Third-party imports
import torch
import numpy as np
from transformers import GPT2LMHeadModel, GPT2Tokenizer

# Local imports
from discord_import import DiscordImporter
from gpt2 import GPT2Trainer
```

### Naming Conventions
- **Functions**: lowercase_with_underscores (e.g., `train_gpt2_model`)
- **Classes**: PascalCase (e.g., `GPT2Trainer`, `DiscordImporter`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `MAX_TOKENS`, `MODEL_PATH`)
- **Private methods**: underscore_prefix (e.g., `_preprocess_data`)
- **Variables**: lowercase_with_underscores (e.g., `model`, `data_loader`)

### Type Hints
```python
from typing import List, Optional, Dict, Any

def train_model(
    model: GPT2LMHeadModel,
    data: List[str],
    epochs: int = 10,
    learning_rate: float = 5e-5
) -> Dict[str, Any]:
    """Train GPT2 model on Discord messages."""
    pass
```

### Error Handling
```python
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_discord_messages(
    messages: List[str],
    max_tokens: int = 1024
) -> Optional[List[str]]:
    """Process Discord messages with error handling."""
    try:
        # Process messages
        result = []
        for message in messages:
            processed = _process_single_message(message, max_tokens)
            if processed:
                result.append(processed)
        return result
    except Exception as e:
        logger.error(f"Error processing messages: {e}")
        return None
```

### Documentation
```python
"""
Discord Message Importer

This module handles importing and processing Discord messages
for GPT2 training.
"""

def train_gpt2_model(
    model_path: str,
    data_path: str,
    epochs: int = 10,
    batch_size: int = 32
) -> None:
    """
    Train GPT2 model on Discord messages.

    Args:
        model_path: Path to pre-trained GPT2 model
        data_path: Path to Discord message data
        epochs: Number of training epochs
        batch_size: Batch size for training

    Raises:
        ValueError: If parameters are invalid
        RuntimeError: If training fails
    """
    pass
```

## Project Structure
```
.
├── discord_import/          # Discord message import and processing
├── gpt2/                    # GPT2 model training and inference
├── tests/                   # Test files
├── requirements.txt         # Python dependencies
├── AGENTS.md               # This file
└── README.md               # Project documentation
```

## Cursor Rules
- No Cursor rules found in `.cursor/rules/` or `.cursorrules`
- No Copilot rules found in `.github/copilot-instructions.md`

## Development Workflow
1. Create feature branches from `main`
2. Write tests before implementation (TDD approach)
3. Run linters and type checks before committing
4. Ensure all tests pass before pushing
5. Update documentation for new features

## Testing Guidelines
- Write unit tests for all functions
- Use pytest fixtures for test setup
- Mock external dependencies (API calls, file I/O)
- Aim for >80% code coverage
- Test edge cases and error conditions

## Model Training Guidelines
- Use pre-trained GPT2 models from Hugging Face
- Monitor training with AIM for loss, metrics, and predictions
- Validate model performance on test set
- Save checkpoints at regular intervals
- Log all training parameters and results

## AIM Monitoring
- Use AIM to track training metrics (loss, accuracy, etc.)
- Monitor model predictions on validation data
- Track data drift and model performance over time
- Set up alerts for abnormal behavior