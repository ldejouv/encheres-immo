"""Tests for tribunal scraper listing parsing and hearing date discovery."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup

from scraper.tribunal_scraper import TribunalScraper


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> BeautifulSoup:
    html = (FIXTURE_DIR / name).read_text(encoding="utf-8")
    return BeautifulSoup(html, "lxml")


def _make_scraper() -> TribunalScraper:
    """Create a TribunalScraper instance without full init (no HTTP session)."""
    scraper = TribunalScraper.__new__(TribunalScraper)
    scraper.logger = logging.getLogger("test_tribunal_scraper")
    return scraper


class TestGetUpcomingHearingUrls:
    """Test the _get_upcoming_hearing_urls method."""

    def setup_method(self):
        self.scraper = _make_scraper()

    def test_extracts_all_hearing_dates(self):
        soup = _load_fixture("tribunal_page.html")
        urls = self.scraper._get_upcoming_hearing_urls(soup)
        assert len(urls) == 5
        assert "/ventes-judiciaires-immobilieres/tj-paris/jeudi-12-fevrier-2026.html" in urls
        assert "/ventes-judiciaires-immobilieres/tj-paris/jeudi-26-fevrier-2026.html" in urls
        assert "/ventes-judiciaires-immobilieres/tj-paris/jeudi-12-mars-2026.html" in urls
        assert "/ventes-judiciaires-immobilieres/tj-paris/jeudi-19-mars-2026.html" in urls
        assert "/ventes-judiciaires-immobilieres/tj-paris/jeudi-26-mars-2026.html" in urls

    def test_excludes_previous_and_empty(self):
        soup = _load_fixture("tribunal_page.html")
        urls = self.scraper._get_upcoming_hearing_urls(soup)
        for url in urls:
            assert "jeudi-5-fevrier-2026" not in url  # Previous audience
        # Empty li items have no links, so should not appear
        assert len(urls) == 5

    def test_strips_hash_fragment(self):
        soup = _load_fixture("tribunal_page.html")
        urls = self.scraper._get_upcoming_hearing_urls(soup)
        for url in urls:
            assert "#" not in url

    def test_no_traversing_section(self):
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        urls = self.scraper._get_upcoming_hearing_urls(soup)
        assert urls == []

    def test_empty_traversing_section(self):
        soup = BeautifulSoup(
            '<html><body><div id="traversing-hearings"></div></body></html>',
            "lxml",
        )
        urls = self.scraper._get_upcoming_hearing_urls(soup)
        assert urls == []


class TestParseListings:
    """Test _parse_listings method via the fixture page."""

    def setup_method(self):
        self.scraper = _make_scraper()

    def test_parse_listings_returns_summaries(self):
        soup = _load_fixture("tribunal_page.html")
        summaries = self.scraper._parse_listings(soup, "/test-url")
        assert len(summaries) == 5

    def test_first_listing_fields(self):
        soup = _load_fixture("tribunal_page.html")
        summaries = self.scraper._parse_listings(soup, "/test-url")
        first = summaries[0]
        assert first.licitor_id == 106898
        assert first.department_code == "13"
        assert first.city == "Cuges-les-Pins"
        assert first.property_type == "Une maison d'habitation"
        assert first.mise_a_prix == 228800

    def test_paris_listings(self):
        soup = _load_fixture("tribunal_page.html")
        summaries = self.scraper._parse_listings(soup, "/test-url")
        paris = [s for s in summaries if s.department_code == "75"]
        assert len(paris) == 4

    def test_empty_page(self):
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        summaries = self.scraper._parse_listings(soup, "/test-url")
        assert summaries == []
