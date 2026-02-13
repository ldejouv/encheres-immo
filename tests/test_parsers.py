"""Unit tests for scraper parsing functions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.parsers import (
    extract_tribunal_slug,
    parse_auction_time,
    parse_department_city,
    parse_french_date,
    parse_gps_from_maps_url,
    parse_licitor_id,
    parse_price,
    parse_surface,
    parse_view_count,
)


class TestParseLicitorId:
    def test_standard_url(self):
        url = "/annonce/10/68/98/vente-aux-encheres/une-maison-d-habitation/cuges-les-pins/bouches-du-rhone/106898.html"
        assert parse_licitor_id(url) == 106898

    def test_short_id(self):
        assert parse_licitor_id("/annonce/1/2/3/vente/type/ville/dept/123.html") == 123

    def test_invalid_raises(self):
        try:
            parse_licitor_id("/not-a-listing")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass


class TestParsePrice:
    def test_euros_with_spaces(self):
        assert parse_price("Mise à prix : 220 000 EUR") == 220000

    def test_simple_number(self):
        assert parse_price("70000") == 70000

    def test_with_euro_sign(self):
        assert parse_price("150 000 €") == 150000

    def test_empty_string(self):
        assert parse_price("") is None

    def test_none(self):
        assert parse_price(None) is None

    def test_no_digits(self):
        assert parse_price("pas de prix") is None


class TestParseGps:
    def test_standard_maps_url(self):
        url = "https://maps.google.fr/maps?q=43.2757149,5.6998372&z=13"
        lat, lng = parse_gps_from_maps_url(url)
        assert abs(lat - 43.2757149) < 0.0001
        assert abs(lng - 5.6998372) < 0.0001

    def test_negative_coordinates(self):
        url = "https://maps.google.fr/maps?q=-12.345,6.789&z=10"
        lat, lng = parse_gps_from_maps_url(url)
        assert abs(lat - (-12.345)) < 0.0001
        assert abs(lng - 6.789) < 0.0001

    def test_no_match(self):
        lat, lng = parse_gps_from_maps_url("https://example.com")
        assert lat is None
        assert lng is None


class TestParseSurface:
    def test_comma_decimal(self):
        assert abs(parse_surface("134,87 m²") - 134.87) < 0.01

    def test_dot_decimal(self):
        assert abs(parse_surface("44.02 m2") - 44.02) < 0.01

    def test_integer(self):
        assert parse_surface("100 m²") == 100.0

    def test_no_match(self):
        assert parse_surface("pas de surface") is None


class TestParseDepartmentCity:
    def test_standard(self):
        dept, city = parse_department_city("75 Paris 16ème")
        assert dept == "75"
        assert city == "Paris 16ème"

    def test_three_digit_dept(self):
        dept, city = parse_department_city("971 Basse-Terre")
        assert dept == "971"
        assert city == "Basse-Terre"

    def test_no_dept(self):
        dept, city = parse_department_city("Paris")
        assert dept is None
        assert city == "Paris"

    def test_whitespace(self):
        dept, city = parse_department_city("  92  Nanterre  ")
        assert dept == "92"
        assert city == "Nanterre"


class TestParseFrenchDate:
    def test_full_date(self):
        assert parse_french_date("jeudi 12 février 2026") == "2026-02-12"

    def test_without_day_name(self):
        assert parse_french_date("3 janvier 2025") == "2025-01-03"

    def test_accented_month(self):
        assert parse_french_date("15 décembre 2024") == "2024-12-15"

    def test_aout(self):
        assert parse_french_date("1 août 2025") == "2025-08-01"

    def test_no_match(self):
        assert parse_french_date("not a date") is None


class TestParseAuctionTime:
    def test_h_format(self):
        assert parse_auction_time("14h00") == "14:00"

    def test_h_with_minutes(self):
        assert parse_auction_time("9h30") == "09:30"

    def test_colon_format(self):
        assert parse_auction_time("14:00") == "14:00"

    def test_no_match(self):
        assert parse_auction_time("pas d'heure") is None


class TestParseViewCount:
    def test_with_spaces(self):
        assert parse_view_count("13 200") == 13200

    def test_simple(self):
        assert parse_view_count("500") == 500

    def test_empty(self):
        assert parse_view_count("") is None


class TestExtractTribunalSlug:
    def test_standard(self):
        url = "/ventes-judiciaires-immobilieres/tj-paris/jeudi-12-fevrier-2026.html"
        assert extract_tribunal_slug(url) == "tj-paris"

    def test_hyphenated_city(self):
        url = "/ventes-judiciaires-immobilieres/tj-aix-en-provence/lundi-10-mars-2026.html"
        assert extract_tribunal_slug(url) == "tj-aix-en-provence"

    def test_no_match(self):
        assert extract_tribunal_slug("/annonce/something.html") is None
