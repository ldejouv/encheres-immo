"""Scrapes historical auction results tribunal by tribunal.

Strategy:
1. From the history index page, discover each tribunal's latest results URL
2. For each tribunal, scrape the results page (listings with final prices)
3. Navigate backwards through past hearings via "Audiences antérieures" link
4. Handle pagination within each hearing (?p=N)
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urljoin

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db.models import ListingSummary
from scraper.base import BaseScraper
from scraper.parsers import parse_licitor_id, parse_price, extract_tribunal_slug


class HistoryScraper(BaseScraper):
    """Scrape past auction results (with final prices) per tribunal."""

    # ----------------------------------------------------------------
    # Discovery: find all tribunal results start URLs
    # ----------------------------------------------------------------

    def discover_tribunal_results_urls(self) -> list[dict]:
        """Parse the history index page to get each tribunal's latest results URL.

        Returns list of dicts with keys: name, slug, url_path, total_count.
        """
        soup = self.fetch(self.config.history_path)

        # The page has a <section id="courts"> (or "search-courts" on results pages)
        courts_section = soup.find("section", id="courts")
        if not courts_section:
            courts_section = soup.find("section", id="search-courts")
        if not courts_section:
            self.logger.warning("No courts section found on history page")
            return []

        tribunals = []
        for link in courts_section.find_all("a", href=True):
            href = link.get("href", "")
            if "/ventes-judiciaires-immobilieres/" not in href:
                continue

            # Extract slug (handles tj-*, chambre-notaires-*, etc.)
            slug_match = re.search(
                r"/ventes-judiciaires-immobilieres/([^/]+)/", href
            )
            if not slug_match:
                continue
            slug = slug_match.group(1)

            # Name is the text without count span
            count_span = link.find("span", class_="Count")
            total_count = 0
            if count_span:
                count_text = count_span.get_text(strip=True)
                if count_text.isdigit():
                    total_count = int(count_text)

            name = link.get_text(strip=True)
            if count_span:
                name = name.replace(count_span.get_text(), "").strip()

            # Clean the URL (remove #hearingDetail anchor if present)
            clean_href = href.split("#")[0]

            tribunals.append({
                "name": name,
                "slug": slug,
                "url_path": clean_href,
                "total_count": total_count,
            })

        self.logger.info(
            "Discovered %d tribunals with history data", len(tribunals)
        )
        return tribunals

    # ----------------------------------------------------------------
    # Parse one results page
    # ----------------------------------------------------------------

    def _parse_result_status(self, result_p) -> tuple[Optional[str], Optional[int], Optional[str]]:
        """Parse a <p class="Result"> element.

        Returns (status, final_price, result_date).
        Formats seen:
            - "05-02-2026 : 58 000 €"  → sold, 58000, "2026-02-05"
            - "Carence d'enchères"       → carence, None, None
            - "Vente non requise"        → non_requise, None, None
        """
        if not result_p:
            return None, None, None

        text = result_p.get_text(strip=True)

        # Check for non-sale statuses
        text_lower = text.lower()
        if "carence" in text_lower:
            return "carence", None, None
        if "non requise" in text_lower:
            return "non_requise", None, None

        # Try to parse sold with price
        price_span = result_p.find("span", class_="PriceNumber")
        final_price = None
        if price_span:
            final_price = parse_price(price_span.get_text(strip=True))

        # Try to parse date: "DD-MM-YYYY" at the start
        result_date = None
        date_match = re.match(r"(\d{2})-(\d{2})-(\d{4})", text)
        if date_match:
            dd, mm, yyyy = date_match.groups()
            result_date = f"{yyyy}-{mm}-{dd}"

        if final_price is not None:
            return "sold", final_price, result_date

        return None, None, None

    def scrape_results_page(self, page_path: str) -> list[ListingSummary]:
        """Scrape one page of auction results.

        Each listing has: location, description, and a <p class="Result"> with
        either a final price or a status (carence/non requise).
        """
        soup = self.fetch(page_path)

        results_list = soup.find("ul", class_="AdResults")
        if not results_list:
            self.logger.debug("No AdResults found on %s", page_path)
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

            # Property type & description
            property_type = None
            description_short = None
            name_span = link.find("span", class_="Name")
            text_span = link.find("span", class_="Text")
            if name_span:
                property_type = name_span.get_text(strip=True)
            if text_span:
                description_short = text_span.get_text(strip=True)

            # Mise à prix (from the listing price, if separate from result)
            mise_a_prix = None
            # On results pages, the price may be in a separate element
            price_div = link.find("div", class_="Price")
            price_p = price_div.find("p", class_="Price") if price_div else None
            if price_p:
                price_span = price_p.find("span", class_="PriceNumber")
                if price_span:
                    mise_a_prix = parse_price(price_span.get_text(strip=True))

            # Result (final price / status)
            result_p = link.find("p", class_="Result")
            result_status, final_price, result_date = self._parse_result_status(result_p)

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
                    final_price=final_price,
                    result_status=result_status,
                    result_date=result_date,
                )
            )

        self.logger.info("Parsed %d listings from %s", len(summaries), page_path)
        return summaries

    # ----------------------------------------------------------------
    # Pagination within a hearing
    # ----------------------------------------------------------------

    def _get_total_pages(self, soup) -> int:
        """Get total number of pages from pagination on a hearing page."""
        page_total = soup.find("span", class_="PageTotal")
        if page_total:
            text = page_total.get_text(strip=True)
            match = re.search(r"(\d+)", text)
            if match:
                return int(match.group(1))
        return 1

    def scrape_hearing_all_pages(self, hearing_path: str) -> list[ListingSummary]:
        """Scrape all pages of a single hearing (handles ?p=N pagination)."""
        # Fetch first page to determine total pages
        soup = self.fetch(hearing_path)
        total_pages = self._get_total_pages(soup)

        # Parse first page
        all_summaries = []
        results_list = soup.find("ul", class_="AdResults")
        if results_list:
            # Re-use the page we already fetched
            all_summaries.extend(self._parse_results_from_soup(soup))

        # Fetch remaining pages
        for p in range(2, total_pages + 1):
            # Add or replace page parameter
            base_path = hearing_path.split("?")[0]
            paged_path = f"{base_path}?p={p}"
            page_summaries = self.scrape_results_page(paged_path)
            all_summaries.extend(page_summaries)

        return all_summaries

    def _parse_results_from_soup(self, soup) -> list[ListingSummary]:
        """Parse results from an already-fetched soup (avoids double-fetching)."""
        results_list = soup.find("ul", class_="AdResults")
        if not results_list:
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
                continue

            dept_code = None
            city = None
            number_span = link.find("span", class_="Number")
            city_span = link.find("span", class_="City")
            if number_span:
                dept_code = number_span.get_text(strip=True)
            if city_span:
                city = city_span.get_text(strip=True)

            property_type = None
            description_short = None
            name_span = link.find("span", class_="Name")
            text_span = link.find("span", class_="Text")
            if name_span:
                property_type = name_span.get_text(strip=True)
            if text_span:
                description_short = text_span.get_text(strip=True)

            mise_a_prix = None
            price_div = link.find("div", class_="Price")
            price_p = price_div.find("p", class_="Price") if price_div else None
            if price_p:
                price_span = price_p.find("span", class_="PriceNumber")
                if price_span:
                    mise_a_prix = parse_price(price_span.get_text(strip=True))

            result_p = link.find("p", class_="Result")
            result_status, final_price, result_date = self._parse_result_status(result_p)

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
                    final_price=final_price,
                    result_status=result_status,
                    result_date=result_date,
                )
            )

        return summaries

    # ----------------------------------------------------------------
    # Navigate backwards through past hearings
    # ----------------------------------------------------------------

    def _get_hearing_dates_from_page(self, soup) -> list[dict]:
        """Extract hearing date links from the traversing section.

        Returns list of dicts: {url_path, label, count}.
        """
        traversing = soup.find("div", id="traversing-hearings")
        if not traversing:
            return []

        hearings = []
        ul = traversing.find("ul")
        if not ul:
            return []

        for li in ul.find_all("li", recursive=False):
            # Skip "Audiences postérieures" and "Audiences antérieures" nav links
            if "Previous" in li.get("class", []) or "Next" in li.get("class", []):
                continue

            link = li.find("a")
            if not link or not link.get("href"):
                continue

            href = link["href"].split("#")[0]
            label = link.get_text(strip=True)

            count = 0
            count_span = link.find("span", class_="Count")
            if count_span:
                count_text = count_span.get_text(strip=True)
                if count_text.isdigit():
                    count = int(count_text)
                label = label.replace(count_span.get_text(), "").strip()

            hearings.append({
                "url_path": href,
                "label": label,
                "count": count,
            })

        return hearings

    def _get_previous_hearings_url(self, soup) -> Optional[str]:
        """Find the 'Audiences antérieures' link to navigate further back."""
        traversing = soup.find("div", id="traversing-hearings")
        if not traversing:
            return None

        ul = traversing.find("ul")
        if not ul:
            return None

        next_li = ul.find("li", class_="Next")
        if not next_li:
            return None

        link = next_li.find("a")
        if link and link.get("href"):
            return link["href"].split("#")[0]

        return None

    # ----------------------------------------------------------------
    # Main: scrape full tribunal history
    # ----------------------------------------------------------------

    def scrape_tribunal_history(
        self,
        start_url: str,
        tribunal_slug: str,
        max_hearings: int = 200,
    ) -> list[ListingSummary]:
        """Scrape all past hearings for a tribunal, walking backwards in time.

        Args:
            start_url: URL of the tribunal's latest results page.
            tribunal_slug: Slug like 'tj-paris'.
            max_hearings: Maximum hearing dates to scrape.

        Returns:
            All ListingSummary objects found across all hearings.
        """
        all_summaries = []
        visited_urls = set()
        hearings_scraped = 0
        current_page_url = start_url

        while current_page_url and hearings_scraped < max_hearings:
            # Avoid loops
            if current_page_url in visited_urls:
                break
            visited_urls.add(current_page_url)

            self.logger.info(
                "[%s] Fetching hearing page: %s (hearings scraped: %d)",
                tribunal_slug, current_page_url, hearings_scraped,
            )

            try:
                soup = self.fetch(current_page_url)
            except Exception as e:
                self.logger.error(
                    "[%s] Failed to fetch %s: %s",
                    tribunal_slug, current_page_url, e,
                )
                break

            # Get all hearing dates listed on this navigation page
            hearing_dates = self._get_hearing_dates_from_page(soup)

            if not hearing_dates:
                # This page itself might be a single hearing - parse its results
                summaries = self._parse_results_from_soup(soup)
                total_pages = self._get_total_pages(soup)

                # Fetch remaining pages of this hearing
                for p in range(2, total_pages + 1):
                    base_path = current_page_url.split("?")[0]
                    paged_path = f"{base_path}?p={p}"
                    page_summaries = self.scrape_results_page(paged_path)
                    summaries.extend(page_summaries)

                all_summaries.extend(summaries)
                hearings_scraped += 1
            else:
                # Parse the current page first (which is the first hearing in the list)
                summaries = self._parse_results_from_soup(soup)
                total_pages = self._get_total_pages(soup)

                for p in range(2, total_pages + 1):
                    base_path = current_page_url.split("?")[0]
                    paged_path = f"{base_path}?p={p}"
                    page_summaries = self.scrape_results_page(paged_path)
                    summaries.extend(page_summaries)

                all_summaries.extend(summaries)
                hearings_scraped += 1

                # Now scrape each other hearing date listed on this page
                for hearing in hearing_dates:
                    if hearings_scraped >= max_hearings:
                        break
                    hearing_url = hearing["url_path"]
                    if hearing_url in visited_urls:
                        continue
                    # Skip if it's the same as the current page
                    if hearing_url.split("?")[0] == current_page_url.split("?")[0]:
                        continue

                    visited_urls.add(hearing_url)
                    try:
                        h_summaries = self.scrape_hearing_all_pages(hearing_url)
                        all_summaries.extend(h_summaries)
                        hearings_scraped += 1
                        self.logger.info(
                            "[%s] Hearing %s: %d listings",
                            tribunal_slug, hearing["label"], len(h_summaries),
                        )
                    except Exception as e:
                        self.logger.error(
                            "[%s] Failed hearing %s: %s",
                            tribunal_slug, hearing_url, e,
                        )

            # Navigate to previous hearings page
            previous_url = self._get_previous_hearings_url(soup)
            current_page_url = previous_url

        self.logger.info(
            "[%s] Total: %d hearings scraped, %d listings found",
            tribunal_slug, hearings_scraped, len(all_summaries),
        )
        return all_summaries
