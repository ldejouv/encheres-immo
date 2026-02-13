"""Scrapes a single property detail page for full listing data."""

from __future__ import annotations

import re
from datetime import date, time

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db.models import ListingDetail
from scraper.base import BaseScraper
from scraper.parsers import (
    parse_french_date,
    parse_gps_from_maps_url,
    parse_licitor_id,
    parse_price,
    parse_surface,
    parse_view_count,
)


class DetailScraper(BaseScraper):
    """Scrape an individual property detail page -> ListingDetail.

    HTML structure (key elements):
        <div class="AdContent" id="ad-106898">
            <p class="PublishingDate">Annonce publiée le <time>...</time></p>
            <p class="Number">106898</p>
            <p class="Court">Tribunal Judiciaire de Paris</p>
            <p class="Type">Vente aux enchères publiques...</p>
            <p class="Date"><time datetime="...">jeudi 12 février 2026 à 14h</time></p>
            <section class="AddressBlock">
                <div class="Lot">
                    <div class="FirstSousLot SousLot">
                        <h2>Une maison d'habitation</h2>
                        <p>Cadastrée section AO n°269</p>
                    </div>
                    <h3>Mise à prix : 228 800 €</h3>
                </div>
                <div class="Location">
                    <p class="City">Cuges-les-Pins (Bouches-du-Rhône)</p>
                    <p class="Street">Lotissement Le Soleil<br/>Route Nationale 8</p>
                    <p class="Map"><a href="https://maps.google.fr/maps?q=LAT,LNG&z=13">...</a></p>
                    <p class="Visits">...</p>
                </div>
            </section>
            <div class="Trusts">
                <div class="Trust">
                    <h3>Maître Jean-Paul Petreschi, Avocat</h3>
                    <p>... Tél.: 01 44 32 07 00</p>
                </div>
            </div>
            <p class="AdditionalText">...RG n°25/00206...</p>
            <div class="Reference">🔎 17.488    ❤ 239</div>
            <div class="Reference">Ferrari & Cie - Réf. A25/0566</div>
        </div>
        <div class="PartnerOffer">
            <div class="PartnerOfferItem">Prix min.<div class="PartnerOfferValue">3 242 €/m²</div></div>
            ...
        </div>
    """

    def scrape(self, url_path: str) -> ListingDetail:
        soup = self.fetch(url_path)
        licitor_id = parse_licitor_id(url_path)

        data: dict = {
            "licitor_id": licitor_id,
            "url_path": url_path,
        }

        ad_content = soup.find("div", class_="AdContent")
        if not ad_content:
            self.logger.warning("No AdContent found for %s", url_path)
            return ListingDetail(**data)

        # Publication date
        pub_time = ad_content.find("p", class_="PublishingDate")
        if pub_time:
            time_el = pub_time.find("time")
            if time_el:
                dt = time_el.get("datetime", "")
                if dt:
                    data["publication_date"] = dt[:10]

        # Court / Tribunal
        court_p = ad_content.find("p", class_="Court")
        if court_p:
            court_text = court_p.get_text(strip=True)
            match = re.search(r"Tribunal\s+Judiciaire\s+(?:de\s+|d['']\s*)([\w\s-]+)", court_text, re.I)
            if match:
                city = match.group(1).strip()
                data["tribunal_name"] = f"TJ {city}"
                data["tribunal_slug"] = "tj-" + re.sub(r"\s+", "-", city.lower())

        # Auction date and time
        date_p = ad_content.find("p", class_="Date")
        if date_p:
            time_el = date_p.find("time")
            if time_el:
                dt_str = time_el.get("datetime", "")
                if "T" in dt_str:
                    date_part, time_part = dt_str.split("T", 1)
                    try:
                        data["auction_date"] = date.fromisoformat(date_part)
                    except ValueError:
                        pass
                    time_match = re.match(r"(\d{2}):(\d{2})", time_part)
                    if time_match:
                        data["auction_time"] = time(int(time_match.group(1)), int(time_match.group(2)))
                elif dt_str:
                    # Fallback: parse from text
                    parsed = parse_french_date(time_el.get_text())
                    if parsed:
                        try:
                            data["auction_date"] = date.fromisoformat(parsed)
                        except ValueError:
                            pass

        # Property type from lot section
        address_block = ad_content.find("section", class_="AddressBlock")
        if address_block:
            lot_div = address_block.find("div", class_="Lot")
            if lot_div:
                # Property type from h2 in SousLot
                sous_lot = lot_div.find("div", class_=re.compile(r"SousLot"))
                if sous_lot:
                    h2 = sous_lot.find("h2")
                    if h2:
                        data["property_type"] = h2.get_text(strip=True)

                    # Description: all text in SousLot except h2
                    desc_parts = []
                    for p in sous_lot.find_all("p"):
                        desc_parts.append(p.get_text(strip=True))
                    if desc_parts:
                        data["description"] = " ".join(desc_parts)

                    # Cadastral reference
                    cadastral_match = re.search(
                        r"[Cc]adastr[ée]e?\s+section\s+([\w\s°n]+)",
                        sous_lot.get_text(),
                    )
                    if cadastral_match:
                        data["cadastral_ref"] = cadastral_match.group(1).strip()

                # Mise à prix from h3 in Lot
                price_h3 = lot_div.find("h3", string=re.compile(r"[Mm]ise\s+[àa]\s+prix"))
                if price_h3:
                    data["mise_a_prix"] = parse_price(price_h3.get_text())

            # Location block
            location_div = address_block.find("div", class_="Location")
            if location_div:
                city_p = location_div.find("p", class_="City")
                if city_p:
                    city_text = city_p.get_text(strip=True)
                    # Parse "Cuges-les-Pins (Bouches-du-Rhône)"
                    dept_match = re.search(r"\(([^)]+)\)", city_text)
                    if dept_match:
                        dept_name = dept_match.group(1)
                        data["city"] = city_text[: city_text.index("(")].strip()
                    else:
                        data["city"] = city_text

                street_p = location_div.find("p", class_="Street")
                if street_p:
                    data["full_address"] = street_p.get_text(separator=", ", strip=True)

                # GPS from maps link
                map_link = location_div.find("a", href=re.compile(r"maps\.google"))
                if map_link:
                    lat, lng = parse_gps_from_maps_url(map_link["href"])
                    data["latitude"] = lat
                    data["longitude"] = lng

                # Visit info
                visit_p = location_div.find("p", class_="Visits")
                if visit_p:
                    data["visit_date"] = visit_p.get_text(strip=True)

        # Lawyer info from Trusts section
        trusts_div = ad_content.find("div", class_="Trusts")
        if trusts_div:
            first_trust = trusts_div.find("div", class_="Trust")
            if first_trust:
                h3 = first_trust.find("h3")
                if h3:
                    data["lawyer_name"] = h3.get_text(strip=True)

                # Phone number
                trust_text = first_trust.get_text()
                phone_match = re.search(r"(\d{2}\s+\d{2}\s+\d{2}\s+\d{2}\s+\d{2})", trust_text)
                if not phone_match:
                    phone_match = re.search(r"(\d{2}\.\d{2}\.\d{2}\.\d{2}\.\d{2})", trust_text)
                if phone_match:
                    data["lawyer_phone"] = phone_match.group(1)

        # Case reference (RG number)
        for additional in ad_content.find_all("p", class_="AdditionalText"):
            text = additional.get_text()
            rg_match = re.search(r"RG\s+n[°o]\s*([\w/]+)", text)
            if rg_match:
                data["case_reference"] = rg_match.group(1)
                break

        # Reference div: views and favorites, and publisher reference
        for ref_div in ad_content.find_all("div", class_="Reference"):
            text = ref_div.get_text(strip=True)
            # Views: 🔎 17.488    ❤ 239
            view_match = re.search(r"(\d[\d\s.,]+)", text)
            fav_match = re.search(r"❤\s*([\d\s.,]+)", text)
            if not fav_match:
                # Unicode heart variants
                fav_match = re.search(r"♥\s*([\d\s.,]+)", text)

            if fav_match:
                data["favorites_count"] = parse_view_count(fav_match.group(1))
                if view_match:
                    data["view_count"] = parse_view_count(view_match.group(1))

            # Publisher reference
            ref_match = re.search(r"Réf\.\s*([\w/]+)", text)
            if ref_match and "case_reference" not in data:
                data["case_reference"] = ref_match.group(1)

        # Surface from description or text
        full_text = ad_content.get_text()
        surface = parse_surface(full_text)
        if surface:
            data["surface_m2"] = surface

        # Price per m² from PartnerOffer section
        partner = soup.find("div", class_="PartnerOffer")
        if partner:
            price_items = partner.find_all("div", class_="PartnerOfferItem")
            for item in price_items:
                label = item.get_text(strip=True)
                value_div = item.find("div", class_="PartnerOfferValue")
                if not value_div:
                    continue
                value = parse_price(value_div.get_text())
                if value:
                    if "min" in label.lower():
                        data["price_per_m2_min"] = float(value)
                    elif "moyen" in label.lower():
                        data["price_per_m2_avg"] = float(value)
                    elif "max" in label.lower():
                        data["price_per_m2_max"] = float(value)

        # Price reduction / baisse
        for text_node in ad_content.find_all(string=re.compile(r"baisse|réduction|diminution", re.I)):
            data["has_price_reduction"] = text_node.strip()
            break

        # Energy rating (DPE)
        dpe_match = re.search(r"DPE\s*[:\s]*([A-G])", full_text, re.I)
        if dpe_match:
            data["energy_rating"] = dpe_match.group(1).upper()

        # Occupancy status
        occ_match = re.search(r"(occup[ée]|libre|vacant)", full_text, re.I)
        if occ_match:
            data["occupancy_status"] = occ_match.group(1).capitalize()

        return ListingDetail(**data)

    def scrape_surface(self, url_path: str) -> float | None:
        """Lightweight scrape: fetch only the surface from a detail page.

        Searches for patterns like "44,02 m²", "134.87 m2" anywhere in
        the page text (AdContent).
        Returns surface in m² as a float, or None if not found.
        """
        soup = self.fetch(url_path)
        ad_content = soup.find("div", class_="AdContent")
        if not ad_content:
            return None

        return parse_surface(ad_content.get_text())

    def scrape_mise_a_prix(self, url_path: str) -> int | None:
        """Lightweight scrape: fetch only the mise a prix from a detail page.

        Looks for:
            <div class="Lot"> <h3>Mise à prix : 40 000 €</h3>
            or <h4>(Mise à prix : 40 000 €)</h4>
        Returns the price as integer euros, or None if not found.
        """
        soup = self.fetch(url_path)
        ad_content = soup.find("div", class_="AdContent")
        if not ad_content:
            return None

        address_block = ad_content.find("section", class_="AddressBlock")
        if not address_block:
            return None

        lot_div = address_block.find("div", class_="Lot")
        if not lot_div:
            return None

        # Primary: h3 "Mise à prix : XX XXX €"
        price_h3 = lot_div.find("h3", string=re.compile(r"[Mm]ise\s+[àa]\s+prix"))
        if price_h3:
            return parse_price(price_h3.get_text())

        # Fallback: h4 "(Mise à prix : XX XXX €)"
        price_h4 = lot_div.find("h4", string=re.compile(r"[Mm]ise\s+[àa]\s+prix"))
        if price_h4:
            return parse_price(price_h4.get_text())

        return None
