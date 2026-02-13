"""Tests for history scraper result parsing using the saved fixture."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup

from scraper.history_scraper import HistoryScraper


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> BeautifulSoup:
    html = (FIXTURE_DIR / name).read_text(encoding="utf-8")
    return BeautifulSoup(html, "lxml")


class TestParseResultStatus:
    """Test the _parse_result_status method."""

    def setup_method(self):
        self.scraper = HistoryScraper.__new__(HistoryScraper)

    def test_sold_with_price_and_date(self):
        soup = BeautifulSoup(
            '<p class="Result">05-02-2026&nbsp;: <span class="PriceNumber">58 000 €</span></p>',
            "lxml",
        )
        result_p = soup.find("p", class_="Result")
        status, price, date = self.scraper._parse_result_status(result_p)
        assert status == "sold"
        assert price == 58000
        assert date == "2026-02-05"

    def test_carence(self):
        soup = BeautifulSoup(
            '<p class="Result">Carence d\'enchères</p>',
            "lxml",
        )
        result_p = soup.find("p", class_="Result")
        status, price, date = self.scraper._parse_result_status(result_p)
        assert status == "carence"
        assert price is None
        assert date is None

    def test_non_requise(self):
        soup = BeautifulSoup(
            '<p class="Result">Vente non requise</p>',
            "lxml",
        )
        result_p = soup.find("p", class_="Result")
        status, price, date = self.scraper._parse_result_status(result_p)
        assert status == "non_requise"
        assert price is None
        assert date is None

    def test_none_element(self):
        status, price, date = self.scraper._parse_result_status(None)
        assert status is None
        assert price is None
        assert date is None

    def test_large_price(self):
        soup = BeautifulSoup(
            '<p class="Result">05-02-2026&nbsp;: <span class="PriceNumber">601 000 €</span></p>',
            "lxml",
        )
        result_p = soup.find("p", class_="Result")
        status, price, date = self.scraper._parse_result_status(result_p)
        assert status == "sold"
        assert price == 601000
        assert date == "2026-02-05"


class TestParseResultsFromFixture:
    """Test parsing the full results_page.html fixture."""

    def setup_method(self):
        self.scraper = HistoryScraper.__new__(HistoryScraper)
        self.soup = _load_fixture("results_page.html")

    def test_parse_results_from_soup(self):
        summaries = self.scraper._parse_results_from_soup(self.soup)
        assert len(summaries) == 5

    def test_first_listing_sold(self):
        summaries = self.scraper._parse_results_from_soup(self.soup)
        first = summaries[0]
        assert first.licitor_id == 106726
        assert first.department_code == "75"
        assert first.city == "Paris 9ème"
        assert first.property_type == "Une pièce"
        assert first.result_status == "sold"
        assert first.final_price == 58000
        assert first.result_date == "2026-02-05"

    def test_carence_listing(self):
        summaries = self.scraper._parse_results_from_soup(self.soup)
        # Third listing is "Carence d'enchères"
        carence = summaries[2]
        assert carence.licitor_id == 106855
        assert carence.result_status == "carence"
        assert carence.final_price is None

    def test_non_requise_listing(self):
        summaries = self.scraper._parse_results_from_soup(self.soup)
        # Fifth listing is "Vente non requise"
        non_req = summaries[4]
        assert non_req.licitor_id == 107032
        assert non_req.result_status == "non_requise"
        assert non_req.final_price is None


class TestHearingNavigation:
    """Test navigation helpers."""

    def setup_method(self):
        self.scraper = HistoryScraper.__new__(HistoryScraper)
        self.soup = _load_fixture("results_page.html")

    def test_get_hearing_dates(self):
        hearings = self.scraper._get_hearing_dates_from_page(self.soup)
        # Should find ~10 hearing dates (excluding Previous/Next nav links)
        assert len(hearings) >= 5
        # First one should be the active hearing
        labels = [h["label"] for h in hearings]
        assert any("5 février 2026" in l for l in labels)

    def test_get_previous_hearings_url(self):
        url = self.scraper._get_previous_hearings_url(self.soup)
        assert url is not None
        assert "tj-paris" in url
        assert "novembre-2025" in url

    def test_total_pages(self):
        total = self.scraper._get_total_pages(self.soup)
        assert total == 2


class TestDiscoverTribunalUrls:
    """Test discovery of tribunal results URLs from the fixture
    (which also serves as the results page with search-courts section)."""

    def setup_method(self):
        self.scraper = HistoryScraper.__new__(HistoryScraper)
        self.soup = _load_fixture("results_page.html")

    def test_find_tribunals_in_courts_section(self):
        courts = self.soup.find("section", id="search-courts")
        assert courts is not None
        links = courts.find_all("a", href=True)
        # Should find multiple tribunal links
        assert len(links) >= 5

    def test_slug_extraction(self):
        """Verify we can extract slugs including non-tj ones."""
        import re
        courts = self.soup.find("section", id="search-courts")
        slugs = set()
        for link in courts.find_all("a", href=True):
            href = link["href"]
            m = re.search(r"/ventes-judiciaires-immobilieres/([^/]+)/", href)
            if m:
                slugs.add(m.group(1))
        assert "tj-paris" in slugs
        assert "tj-versailles" in slugs
        assert "chambre-notaires-paris" in slugs
