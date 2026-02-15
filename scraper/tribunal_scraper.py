"""Scrapes a tribunal hearing page to get listing summaries."""

from __future__ import annotations

import re

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db.models import ListingSummary
from scraper.base import BaseScraper
from scraper.parsers import parse_licitor_id, parse_price


class TribunalScraper(BaseScraper):
    """Scrape a tribunal page -> list of ListingSummary.

    HTML structure per listing:
        <li>
            <a class="Ad" href="/annonce/.../107090.html" title="...">
                <p class="Location">
                    <span class="Number">75</span>
                    <span class="City">Paris 16ème</span>
                </p>
                <p class="Description">
                    <span class="Name">Un appartement</span>
                    <span class="Text">de 58,61 m², au 4ème étage...</span>
                </p>
                <div class="Footer">
                    <div class="Price">
                        <p class="Initial">Mise à prix : <span class="PriceNumber">220 000 €</span></p>
                    </div>
                </div>
            </a>
            <p class="PublishingDate"><span>Mercredi 31 décembre</span></p>
        </li>
    """

    def _get_upcoming_hearing_urls(self, soup) -> list[str]:
        """Extract additional hearing date URLs from traversing-hearings section."""
        traversing = soup.find("div", id="traversing-hearings")
        if not traversing:
            return []
        ul = traversing.find("ul")
        if not ul:
            return []
        urls: list[str] = []
        for li in ul.find_all("li", recursive=False):
            classes = li.get("class", [])
            if any(c in classes for c in ("Previous", "Next", "Empty")):
                continue
            link = li.find("a")
            if link and link.get("href"):
                urls.append(link["href"].split("#")[0])
        return urls

    def _parse_listings(self, soup, source_url: str) -> list[ListingSummary]:
        """Parse listing summaries from a single page's AdResults list."""
        results_list = soup.find("ul", class_="AdResults")
        if not results_list:
            self.logger.warning("No AdResults found on %s", source_url)
            return []

        summaries = []
        for li in results_list.find_all("li", recursive=False):
            link = li.find("a", class_=re.compile(r"Ad"))
            if not link:
                continue

            href = link.get("href", "")
            if not href.startswith("/annonce/"):
                continue

            try:
                licitor_id = parse_licitor_id(href)
            except ValueError:
                self.logger.warning("Could not parse ID from %s", href)
                continue

            # Location
            dept_code = None
            city = None
            number_span = link.find("span", class_="Number")
            city_span = link.find("span", class_="City")
            if number_span:
                dept_code = number_span.get_text(strip=True)
            if city_span:
                city = city_span.get_text(strip=True)

            # Property type and description
            property_type = None
            description_short = None
            name_span = link.find("span", class_="Name")
            text_span = link.find("span", class_="Text")
            if name_span:
                property_type = name_span.get_text(strip=True)
            if text_span:
                description_short = text_span.get_text(strip=True)

            # Price
            mise_a_prix = None
            price_span = link.find("span", class_="PriceNumber")
            if price_span:
                mise_a_prix = parse_price(price_span.get_text(strip=True))

            # Publication date
            pub_date = None
            pub_p = li.find("p", class_="PublishingDate")
            if pub_p:
                pub_date = pub_p.get_text(strip=True) or None

            summaries.append(
                ListingSummary(
                    licitor_id=licitor_id,
                    url_path=href,
                    property_type=property_type,
                    department_code=dept_code,
                    city=city,
                    mise_a_prix=mise_a_prix,
                    description_short=description_short,
                    publication_date=pub_date,
                )
            )

        self.logger.info("Found %d listings on %s", len(summaries), source_url)
        return summaries

    def scrape(self, tribunal_url: str, _visited: set | None = None) -> list[ListingSummary]:
        is_initial = _visited is None
        if is_initial:
            _visited = set()

        base_url = tribunal_url.split("?")[0].split("#")[0]
        if base_url in _visited:
            return []
        _visited.add(base_url)

        # Scrape all pages of this hearing date (pagination = while loop)
        summaries: list[ListingSummary] = []
        current_url = tribunal_url
        first_soup = None

        while current_url:
            soup = self.fetch(current_url)
            if first_soup is None:
                first_soup = soup
            summaries.extend(self._parse_listings(soup, current_url))

            next_link = soup.find("a", class_="Next PageNav")
            if next_link and next_link.get("href"):
                current_url = next_link["href"]
                self.logger.info("Following pagination to %s", current_url)
            else:
                current_url = None

        # Discover and scrape other hearing dates (only from the initial call)
        if is_initial and first_soup:
            for url in self._get_upcoming_hearing_urls(first_soup):
                summaries.extend(self.scrape(url, _visited=_visited))

        return summaries
