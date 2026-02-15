"""Tests for tribunal scraper listing parsing and hearing date discovery."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup

from scraper.tribunal_scraper import TribunalScraper


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> BeautifulSoup:
    html = (FIXTURE_DIR / name).read_text(encoding="utf-8")
    return BeautifulSoup(html, "lxml")


class TestGetUpcomingHearingUrls:
    """Test the _get_upcoming_hearing_urls method."""

    def setup_method(self):
        self.scraper = TribunalScraper.__new__(TribunalScraper)

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
    """Test listing parsing from the fixture page."""

    def setup_method(self):
        self.scraper = TribunalScraper.__new__(TribunalScraper)

    def test_parses_listings_from_fixture(self):
        soup = _load_fixture("tribunal_page.html")
        results_list = soup.find("ul", class_="AdResults")
        assert results_list is not None
        listings = results_list.find_all("li", recursive=False)
        assert len(listings) == 5

    def test_first_listing_details(self):
        """Verify the first listing in the fixture is parsed correctly."""
        soup = _load_fixture("tribunal_page.html")
        results_list = soup.find("ul", class_="AdResults")
        first_li = results_list.find_all("li", recursive=False)[0]
        link = first_li.find("a")
        assert link is not None
        assert "106898" in link["href"]
        assert first_li.find("span", class_="Number").get_text(strip=True) == "13"
        assert first_li.find("span", class_="City").get_text(strip=True) == "Cuges-les-Pins"
