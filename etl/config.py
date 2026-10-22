"""Application settings, read from environment variables (see .env.example)."""
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# 1 AZN = 5000 AZM since 2006-01-01; earlier bulletins are in old manat (README, finding #6)
DENOMINATION_DATE = date(2006, 1, 1)


def load_dotenv(path: Path = Path(".env")) -> None:
    """Minimal .env loader: KEY=VALUE lines, existing environment variables win."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class Settings:
    cbar_base_url: str
    request_timeout: float
    request_delay: float
    max_retries: int
    retry_backoff: float
    log_level: str
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "etl"
    postgres_password: str = "etl"
    postgres_db: str = "cbar"

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        env = os.environ.get
        return cls(
            cbar_base_url=env("CBAR_BASE_URL", "https://www.cbar.az/currencies").rstrip("/"),
            request_timeout=float(env("REQUEST_TIMEOUT", "15")),
            request_delay=float(env("REQUEST_DELAY", "0.2")),
            max_retries=int(env("MAX_RETRIES", "3")),
            retry_backoff=float(env("RETRY_BACKOFF", "1.0")),
            log_level=env("LOG_LEVEL", "INFO").upper(),
            postgres_host=env("POSTGRES_HOST", "localhost"),
            postgres_port=int(env("POSTGRES_PORT", "5432")),
            postgres_user=env("POSTGRES_USER", "etl"),
            postgres_password=env("POSTGRES_PASSWORD", "etl"),
            postgres_db=env("POSTGRES_DB", "cbar"),
        )

    def conninfo(self) -> dict[str, str | int]:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "user": self.postgres_user,
            "password": self.postgres_password,
            "dbname": self.postgres_db,
        }
