"""Pure parsing functions for extracting structured data from Licitor HTML pages."""

from __future__ import annotations

import re
from typing import Optional

MONTHS_FR = {
    "janvier": 1,
    "fevrier": 2,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
    "décembre": 12,
}

_MONTH_PATTERN = (
    "janvier|f[eé]vrier|mars|avril|mai|juin|"
    "juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre"
)


def parse_licitor_id(url_path: str) -> int:
    """Extract numeric ID from URL like /annonce/.../106898.html -> 106898."""
    match = re.search(r"/(\d+)\.html$", url_path)
    if match:
        return int(match.group(1))
    raise ValueError(f"Cannot extract licitor_id from: {url_path}")


def parse_price(text: str) -> Optional[int]:
    """Parse price text -> integer euros.

    Handles: '220 000 EUR', '220,000', '220000 €', 'Mise à prix : 70 000 EUR'.
    """
    if not text:
        return None
    # Remove everything except digits
    cleaned = re.sub(r"[^\d]", "", text)
    return int(cleaned) if cleaned else None


def parse_gps_from_maps_url(url: str) -> tuple[Optional[float], Optional[float]]:
    """Extract lat/lng from Google Maps URL.

    Pattern: https://maps.google.fr/maps?q=48.8534,2.2754&z=13
    """
    match = re.search(r"q=([-\d.]+),([-\d.]+)", url)
    if match:
        return float(match.group(1)), float(match.group(2))
    return None, None


def parse_surface(text: str) -> Optional[float]:
    """Parse surface from text like '44,02 m²' or '134.87 m2' -> float."""
    match = re.search(r"([\d.,]+)\s*m[²2]", text)
    if match:
        return float(match.group(1).replace(",", "."))
    return None


def parse_department_city(location_text: str) -> tuple[Optional[str], Optional[str]]:
    """Parse '75 Paris 16ème' -> ('75', 'Paris 16ème')."""
    text = location_text.strip()
    match = re.match(r"^(\d{2,3})\s+(.+)$", text)
    if match:
        return match.group(1), match.group(2).strip()
    return None, text


def parse_french_date(text: str) -> Optional[str]:
    """Parse French date text -> ISO format 'YYYY-MM-DD'.

    Handles: 'jeudi 12 février 2026', '12 mars', '3 janvier 2025'.
    """
    pattern = (
        r"(\d{1,2})\s+(" + _MONTH_PATTERN + r")"
        r"(?:\s+(\d{4}))?"
    )
    match = re.search(pattern, text.lower())
    if not match:
        return None
    day = int(match.group(1))
    month_str = match.group(2)
    month = MONTHS_FR.get(month_str)
    if month is None:
        return None
    year = int(match.group(3)) if match.group(3) else None
    if year is None:
        from datetime import date

        year = date.today().year
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_auction_time(text: str) -> Optional[str]:
    """Parse auction time like '14h00', '9h30', '14:00' -> 'HH:MM'."""
    match = re.search(r"(\d{1,2})\s*[hH:]\s*(\d{2})", text)
    if match:
        return f"{int(match.group(1)):02d}:{match.group(2)}"
    return None


def parse_view_count(text: str) -> Optional[int]:
    """Parse engagement count like '13 200' or '13200' -> 13200."""
    cleaned = re.sub(r"[^\d]", "", text)
    return int(cleaned) if cleaned else None


def extract_tribunal_slug(url_path: str) -> Optional[str]:
    """Extract 'tj-paris' from '/ventes-judiciaires-immobilieres/tj-paris/...'."""
    match = re.search(r"/ventes-judiciaires-immobilieres/(tj-[^/]+)/", url_path)
    return match.group(1) if match else None
