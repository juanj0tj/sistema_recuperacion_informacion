from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import os


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Paths
    PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DIR: Path = DATA_DIR / "raw"
    INDEX_DIR: Path = DATA_DIR / "indexes"

    # Parámetros RI
    DEFAULT_LANGUAGE: str = "spanish"
    DEFAULT_QUERY_LANGUAGE: str = "spanish"
    TOP_K: int = 20
    MIN_TOKEN_LEN: int = 2
    MIN_DF: int = 2
    MAX_DF_RATIO: float = 0.5
    INDEX_WORKERS: int = os.cpu_count() or 1
    INDEX_BLOCK_DOCS: int = 10_000
    INDEX_MAX_IN_FLIGHT: int = 0  # 0 = auto
    INDEX_MAX_TASKS_PER_CHILD: int = 10  # 0 = desactivar reciclaje de workers
    INDEX_KEEP_BLOCKS: bool = False


settings = Settings()
