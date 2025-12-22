from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Paths
    PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DIR: Path = DATA_DIR / "raw"
    PROCESSED_DIR: Path = DATA_DIR / "processed"
    INDEX_DIR: Path = DATA_DIR / "indexes"

    # Parámetros RI
    LANGUAGE: str = "spanish"
    TOP_K: int = 10
    MIN_TOKEN_LEN: int = 2


settings = Settings()
