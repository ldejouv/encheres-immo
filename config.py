from pathlib import Path
from dataclasses import dataclass, field

BASE_DIR = Path(__file__).resolve().parent


@dataclass
class ScraperConfig:
    base_url: str = "https://www.licitor.com"
    index_path: str = "/ventes-aux-encheres-immobilieres/france.html"
    history_path: str = "/historique-des-adjudications.html"

    # Rate limiting
    min_delay: float = 1.5
    max_delay: float = 3.0
    max_retries: int = 3
    retry_backoff: float = 2.0
    timeout: int = 30

    user_agent: str = (
        "Mozilla/5.0 (compatible; EnchImmoBot/1.0; "
        "+mailto:contact@encheres-immo.local)"
    )


@dataclass
class DBConfig:
    db_path: Path = field(default_factory=lambda: BASE_DIR / "data" / "encheres.db")
    schema_path: Path = field(default_factory=lambda: BASE_DIR / "db" / "schema.sql")


@dataclass
class AppConfig:
    scraper: ScraperConfig = field(default_factory=ScraperConfig)
    db: DBConfig = field(default_factory=DBConfig)


config = AppConfig()
