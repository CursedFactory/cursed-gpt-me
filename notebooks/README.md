# Notebooks

This directory contains Jupyter notebooks for exploring and working with Discord messages.

## Setup

To use these notebooks, you need to have JupyterLab installed. The dependencies are managed by uv.

```bash
# Install dependencies
uv add jupyterlab ipykernel notebook

# Install the kernel for this project
python -m ipykernel install --user --name=gpt-discord-small --package-dir=.
```

## Available Notebooks

### discord_import.ipynb

Demonstrates how to:
- Initialize the DiscordLoader
- Find Discord message JSON files recursively
- Load and inspect message content
- Extract text for GPT2 training
- Configure logging

## Running Notebooks

```bash
# Start JupyterLab
jupyter lab

# Or run a specific notebook
jupyter notebook discord_import.ipynb
```

## Usage

1. Open a notebook in JupyterLab
2. Run cells sequentially using the Play button or Shift+Enter
3. Modify parameters and re-run cells as needed

## Dependencies

All dependencies are managed by uv. Add new dependencies using:

```bash
uv add <package-name>
```