from pathlib import Path
from app.core.config import settings


def ensure_dirs():
    settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
    settings.RAW_DIR.mkdir(parents=True, exist_ok=True)
    settings.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    settings.INDEX_DIR.mkdir(parents=True, exist_ok=True)


def index_file_path() -> Path:
    meta_path = settings.INDEX_DIR / "index.meta.json"
    if meta_path.exists():
        return meta_path
    gz_path = settings.INDEX_DIR / "index.json.gz"
    if gz_path.exists():
        return gz_path
    return settings.INDEX_DIR / "index.json"
