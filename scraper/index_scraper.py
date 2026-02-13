"""Scrapes the France index page to get all tribunals with auction counts."""

from __future__ import annotations

import re

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db.models import TribunalInfo
from scraper.base import BaseScraper
from scraper.parsers import extract_tribunal_slug


class IndexScraper(BaseScraper):
    """Scrape /ventes-aux-encheres-immobilieres/france.html -> list of TribunalInfo."""

    def scrape(self) -> list[TribunalInfo]:
        soup = self.fetch(self.config.index_path)

        courts_section = soup.find("section", id="courts")
        if not courts_section:
            self.logger.error("Could not find #courts section")
            return []

        tribunals = []
        current_region = "Unknown"

        for h3 in courts_section.find_all("h3"):
            span = h3.find("span")
            if span:
                current_region = span.get_text(strip=True)

            # All tribunal links in the <ul> following this h3
            parent_li = h3.find_parent("li")
            if not parent_li:
                continue

            for link in parent_li.find_all("a", href=re.compile(r"/ventes-judiciaires-immobilieres/tj-")):
                href = link.get("href", "")
                slug = extract_tribunal_slug(href)
                if not slug:
                    continue

                # Name is the text without the count span
                count_span = link.find("span", class_="Count")
                count = 0
                if count_span:
                    count_text = count_span.get_text(strip=True)
                    if count_text.isdigit():
                        count = int(count_text)

                name = link.get_text(strip=True)
                if count_span:
                    name = name.replace(count_span.get_text(), "").strip()

                tribunals.append(
                    TribunalInfo(
                        name=name,
                        slug=slug,
                        region=current_region,
                        auction_count=count,
                        url_path=href,
                    )
                )

        self.logger.info("Found %d tribunals", len(tribunals))
        return tribunals
