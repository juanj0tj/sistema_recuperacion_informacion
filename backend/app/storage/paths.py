from pathlib import Path
from app.core.config import settings


def ensure_dirs():
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.RAW_DIR.mkdir(parents=True, exist_ok=True)
    settings.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    settings.INDEX_DIR.mkdir(parents=True, exist_ok=True)


def index_file_path() -> Path:
    return settings.INDEX_DIR / "index.json"
