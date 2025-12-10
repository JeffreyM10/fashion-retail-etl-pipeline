# src/logging_config.py
import logging
from pathlib import Path

# logs/ingestion.log (relative to project root)
LOG_FILE = Path(__file__).resolve().parents[1] / "logs" / "ingestion.log"
LOG_FILE.parent.mkdir(exist_ok=True)

# Configure root logger once
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
        logging.StreamHandler(),  # also show logs in the terminal
    ],
)

def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger."""
    return logging.getLogger(name)
