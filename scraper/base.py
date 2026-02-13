"""Base scraper with session management, rate limiting, and retry logic."""

from __future__ import annotations

import logging
import random
import time

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config import ScraperConfig, config as app_config


class BaseScraper:
    """Handles session management, rate limiting, retries, and logging."""

    def __init__(self, scraper_config: ScraperConfig | None = None):
        self.config = scraper_config or app_config.scraper
        self.session = self._build_session()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_request_time = 0.0

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": self.config.user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr-FR,fr;q=0.9",
            }
        )
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        delay = random.uniform(self.config.min_delay, self.config.max_delay)
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_time = time.time()

    def fetch(self, path: str) -> BeautifulSoup:
        """Fetch a page respecting rate limits. Returns parsed BeautifulSoup."""
        self._rate_limit()
        url = self.config.base_url + path
        self.logger.info("Fetching %s", url)
        response = self.session.get(url, timeout=self.config.timeout)
        response.raise_for_status()
        response.encoding = "utf-8"
        return BeautifulSoup(response.text, "lxml")
