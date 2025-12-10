import yaml
import os
from pathlib import Path
from dotenv import load_dotenv


CONFIG_PATH = Path(__file__).parent.parent / "config" / "sources.yml"
load_dotenv()

def load_sources_config():
    """
    Loads the sources.yml configuration file.
    Returns a dictionary with all ingestion source definitions.
    """
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return data

def get_db_url():
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL not set. Please create a .env file.")
    return db_url