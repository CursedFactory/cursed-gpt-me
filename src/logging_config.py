import logging
from rich.logging import RichHandler

def get_logger(name: str) -> logging.Logger:
    """Get a logger with Rich console output."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = RichHandler(
            show_time=True,
            show_path=False,
            rich_tracebacks=True,
            markup=False,
            log_time_format="%Y-%m-%d %H:%M:%S",
        )
        formatter = logging.Formatter("{message}", style="{")
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger
