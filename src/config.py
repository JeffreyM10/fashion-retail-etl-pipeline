import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config" / "sources.yml"

def load_sources_config():
    """
    Loads the sources.yml configuration file.
    Returns a dictionary with all ingestion source definitions.
    """
    with open(CONFIG_PATH, "r") as f:
        data = yaml.safe_load(f)
    return data

def get_db_url():
    """
    Convenience helper to fetch the database URL from YAML.
    """
    cfg = load_sources_config()
    return cfg["defaults"]["db_url"]