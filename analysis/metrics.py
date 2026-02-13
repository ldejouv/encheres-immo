"""Auction data analysis and metrics computation."""

from __future__ import annotations

import pandas as pd

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db.database import Database


class AuctionMetrics:
    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    def get_upcoming_listings(self, filters: dict | None = None) -> pd.DataFrame:
        """Get upcoming auction listings with optional filters."""
        query = """
            SELECT l.*, t.name as tribunal_name, t.region
            FROM listings l
            LEFT JOIN tribunals t ON t.id = l.tribunal_id
            WHERE l.status = 'upcoming'
        """
        params = []
        if filters:
            if filters.get("department_codes"):
                placeholders = ",".join("?" * len(filters["department_codes"]))
                query += f" AND l.department_code IN ({placeholders})"
                params.extend(filters["department_codes"])
            if filters.get("min_price") is not None:
                query += " AND l.mise_a_prix >= ?"
                params.append(filters["min_price"])
            if filters.get("max_price") is not None:
                query += " AND l.mise_a_prix <= ?"
                params.append(filters["max_price"])
            if filters.get("property_types"):
                conditions = " OR ".join(
                    "LOWER(l.property_type) LIKE ?" for _ in filters["property_types"]
                )
                query += f" AND ({conditions})"
                params.extend(f"%{t.lower()}%" for t in filters["property_types"])
            if filters.get("regions"):
                placeholders = ",".join("?" * len(filters["regions"]))
                query += f" AND t.region IN ({placeholders})"
                params.extend(filters["regions"])

        query += " ORDER BY l.auction_date ASC"

        with self.db.connect() as conn:
            return pd.read_sql(query, conn, params=params)

    def get_historical_listings(self, filters: dict | None = None) -> pd.DataFrame:
        """Get all historical listings with result data and optional filters.

        Supports filters: department_codes, regions, property_types,
        result_statuses, min_price, max_price, min_final, max_final,
        cities, tribunal_names.
        """
        query = """
            SELECT l.licitor_id, l.url_path, l.property_type,
                   l.department_code, l.city, l.mise_a_prix,
                   l.surface_m2, l.description,
                   l.auction_date, l.result_date,
                   l.result_status, l.final_price,
                   l.status, l.is_historical,
                   t.name as tribunal_name, t.region,
                   CASE WHEN l.final_price > 0 AND l.mise_a_prix > 0
                        THEN CAST(l.final_price AS REAL) / l.mise_a_prix
                        ELSE NULL
                   END as ratio
            FROM listings l
            LEFT JOIN tribunals t ON t.id = l.tribunal_id
            WHERE l.result_status IS NOT NULL
        """
        params = []
        if filters:
            if filters.get("department_codes"):
                ph = ",".join("?" * len(filters["department_codes"]))
                query += f" AND l.department_code IN ({ph})"
                params.extend(filters["department_codes"])
            if filters.get("regions"):
                ph = ",".join("?" * len(filters["regions"]))
                query += f" AND t.region IN ({ph})"
                params.extend(filters["regions"])
            if filters.get("property_types"):
                conds = " OR ".join(
                    "LOWER(l.property_type) LIKE ?" for _ in filters["property_types"]
                )
                query += f" AND ({conds})"
                params.extend(f"%{t.lower()}%" for t in filters["property_types"])
            if filters.get("result_statuses"):
                ph = ",".join("?" * len(filters["result_statuses"]))
                query += f" AND l.result_status IN ({ph})"
                params.extend(filters["result_statuses"])
            if filters.get("cities"):
                conds = " OR ".join(
                    "LOWER(l.city) LIKE ?" for _ in filters["cities"]
                )
                query += f" AND ({conds})"
                params.extend(f"%{c.lower()}%" for c in filters["cities"])
            if filters.get("tribunal_names"):
                ph = ",".join("?" * len(filters["tribunal_names"]))
                query += f" AND t.name IN ({ph})"
                params.extend(filters["tribunal_names"])
            if filters.get("min_price") is not None:
                query += " AND l.mise_a_prix >= ?"
                params.append(filters["min_price"])
            if filters.get("max_price") is not None:
                query += " AND l.mise_a_prix <= ?"
                params.append(filters["max_price"])
            if filters.get("min_final") is not None:
                query += " AND l.final_price >= ?"
                params.append(filters["min_final"])
            if filters.get("max_final") is not None:
                query += " AND l.final_price <= ?"
                params.append(filters["max_final"])
            if filters.get("min_surface") is not None:
                query += " AND l.surface_m2 >= ?"
                params.append(filters["min_surface"])
            if filters.get("max_surface") is not None:
                query += " AND l.surface_m2 <= ?"
                params.append(filters["max_surface"])

        query += " ORDER BY l.result_date DESC NULLS LAST, l.licitor_id DESC"

        with self.db.connect() as conn:
            return pd.read_sql(query, conn, params=params)

    def get_historical_filter_options(self) -> dict:
        """Get distinct values for filter dropdowns."""
        with self.db.connect() as conn:
            depts = [r[0] for r in conn.execute(
                "SELECT DISTINCT department_code FROM listings WHERE result_status IS NOT NULL AND department_code IS NOT NULL ORDER BY department_code"
            ).fetchall()]
            cities = [r[0] for r in conn.execute(
                "SELECT DISTINCT city FROM listings WHERE result_status IS NOT NULL AND city IS NOT NULL ORDER BY city"
            ).fetchall()]
            types = [r[0] for r in conn.execute(
                "SELECT DISTINCT property_type FROM listings WHERE result_status IS NOT NULL AND property_type IS NOT NULL ORDER BY property_type"
            ).fetchall()]
            tribunals = [r[0] for r in conn.execute(
                "SELECT DISTINCT t.name FROM listings l JOIN tribunals t ON t.id = l.tribunal_id WHERE l.result_status IS NOT NULL ORDER BY t.name"
            ).fetchall()]
            regions = [r[0] for r in conn.execute(
                "SELECT DISTINCT t.region FROM listings l JOIN tribunals t ON t.id = l.tribunal_id WHERE l.result_status IS NOT NULL AND t.region IS NOT NULL ORDER BY t.region"
            ).fetchall()]
            statuses = [r[0] for r in conn.execute(
                "SELECT DISTINCT result_status FROM listings WHERE result_status IS NOT NULL ORDER BY result_status"
            ).fetchall()]
            price_range = conn.execute(
                "SELECT MIN(mise_a_prix), MAX(mise_a_prix), MIN(final_price), MAX(final_price) FROM listings WHERE result_status IS NOT NULL"
            ).fetchone()
            surface_range = conn.execute(
                "SELECT MIN(surface_m2), MAX(surface_m2) FROM listings WHERE result_status IS NOT NULL AND surface_m2 IS NOT NULL AND surface_m2 > 0"
            ).fetchone()
            return {
                "departments": depts,
                "cities": cities,
                "property_types": types,
                "tribunal_names": tribunals,
                "regions": regions,
                "result_statuses": statuses,
                "min_mise_a_prix": price_range[0] or 0,
                "max_mise_a_prix": price_range[1] or 1_000_000,
                "min_final_price": price_range[2] or 0,
                "max_final_price": price_range[3] or 1_000_000,
                "min_surface": surface_range[0] or 0 if surface_range[0] else 0,
                "max_surface": surface_range[1] or 1000 if surface_range[1] else 1000,
            }

    def mise_a_prix_distribution(self) -> pd.DataFrame:
        """Distribution of starting prices by property type and department."""
        query = """
            SELECT mise_a_prix, property_type, department_code, city,
                   auction_date, surface_m2, status
            FROM listings
            WHERE mise_a_prix IS NOT NULL
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def price_per_m2_analysis(self) -> pd.DataFrame:
        """Compare mise_a_prix/m² vs regional average price/m².

        discount_ratio < 1.0 means starting price is below market.
        """
        query = """
            SELECT l.licitor_id, l.city, l.department_code,
                   l.property_type, l.mise_a_prix, l.surface_m2,
                   l.price_per_m2_avg, l.auction_date, l.status, l.url_path,
                   CASE WHEN l.surface_m2 > 0 AND l.price_per_m2_avg > 0
                        THEN (CAST(l.mise_a_prix AS REAL) / l.surface_m2)
                              / l.price_per_m2_avg
                        ELSE NULL
                   END as discount_ratio,
                   CASE WHEN l.surface_m2 > 0
                        THEN CAST(l.mise_a_prix AS REAL) / l.surface_m2
                        ELSE NULL
                   END as price_per_m2
            FROM listings l
            WHERE l.surface_m2 IS NOT NULL
              AND l.mise_a_prix IS NOT NULL
              AND l.surface_m2 > 0
            ORDER BY discount_ratio ASC NULLS LAST
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def temporal_trends(self, granularity: str = "month") -> pd.DataFrame:
        """Auction counts and average prices over time."""
        date_expr = {
            "month": "strftime('%Y-%m', auction_date)",
            "week": "strftime('%Y-W%W', auction_date)",
            "year": "strftime('%Y', auction_date)",
        }
        expr = date_expr.get(granularity, date_expr["month"])
        query = f"""
            SELECT {expr} as period,
                   COUNT(*) as auction_count,
                   AVG(mise_a_prix) as avg_mise_a_prix,
                   MIN(mise_a_prix) as min_mise_a_prix,
                   MAX(mise_a_prix) as max_mise_a_prix,
                   AVG(CASE WHEN surface_m2 > 0
                        THEN CAST(mise_a_prix AS REAL) / surface_m2
                        ELSE NULL END) as avg_price_per_m2
            FROM listings
            WHERE auction_date IS NOT NULL AND mise_a_prix IS NOT NULL
            GROUP BY period
            ORDER BY period
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def department_stats(self) -> pd.DataFrame:
        """Statistics per department."""
        query = """
            SELECT department_code,
                   COUNT(*) as total_listings,
                   SUM(CASE WHEN status = 'upcoming' THEN 1 ELSE 0 END) as upcoming,
                   AVG(mise_a_prix) as avg_mise_a_prix,
                   AVG(CASE WHEN surface_m2 > 0
                        THEN CAST(mise_a_prix AS REAL) / surface_m2
                        ELSE NULL END) as avg_price_per_m2
            FROM listings
            WHERE department_code IS NOT NULL AND mise_a_prix IS NOT NULL
            GROUP BY department_code
            ORDER BY total_listings DESC
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def popularity_analysis(self) -> pd.DataFrame:
        """Most viewed and favorited listings."""
        query = """
            SELECT licitor_id, city, department_code, property_type,
                   mise_a_prix, surface_m2, view_count, favorites_count,
                   auction_date, status, url_path
            FROM listings
            WHERE view_count IS NOT NULL OR favorites_count IS NOT NULL
            ORDER BY COALESCE(favorites_count, 0) DESC
            LIMIT 100
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def adjudication_ratio_analysis(self) -> pd.DataFrame:
        """Compute ratio final_price / mise_a_prix from all available sources.

        Combines:
        1. Scraped results (listings.final_price from results pages)
        2. Manually entered results (adjudication_results table)
        """
        query = """
            SELECT licitor_id, city, department_code, property_type,
                   mise_a_prix, surface_m2, final_price, ratio,
                   price_source, auction_date, result_status
            FROM (
                -- Source 1: scraped final prices
                SELECT l.licitor_id, l.city, l.department_code,
                       l.property_type, l.mise_a_prix, l.surface_m2,
                       l.final_price,
                       CAST(l.final_price AS REAL) / l.mise_a_prix as ratio,
                       'scraped' as price_source,
                       l.auction_date, l.result_status
                FROM listings l
                WHERE l.final_price IS NOT NULL
                  AND l.final_price > 0
                  AND l.mise_a_prix > 0

                UNION ALL

                -- Source 2: manually entered final prices (excluding duplicates)
                SELECT l.licitor_id, l.city, l.department_code,
                       l.property_type, l.mise_a_prix, l.surface_m2,
                       ar.final_price,
                       CAST(ar.final_price AS REAL) / l.mise_a_prix as ratio,
                       ar.price_source,
                       l.auction_date, l.result_status
                FROM listings l
                JOIN adjudication_results ar ON ar.listing_id = l.id
                WHERE l.mise_a_prix > 0
                  AND ar.final_price > 0
                  AND (l.final_price IS NULL OR l.final_price = 0)
            )
            ORDER BY ratio ASC
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def result_status_breakdown(self) -> pd.DataFrame:
        """Breakdown of auction outcomes: sold, carence, non_requise."""
        query = """
            SELECT result_status,
                   COUNT(*) as count,
                   AVG(CASE WHEN result_status = 'sold' THEN final_price ELSE NULL END) as avg_final_price,
                   AVG(mise_a_prix) as avg_mise_a_prix
            FROM listings
            WHERE result_status IS NOT NULL
            GROUP BY result_status
            ORDER BY count DESC
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def geographic_data(self) -> pd.DataFrame:
        """All listings with GPS coordinates for map display."""
        query = """
            SELECT l.licitor_id, l.city, l.department_code, l.property_type,
                   l.mise_a_prix, l.surface_m2, l.latitude, l.longitude,
                   l.auction_date, l.status, l.url_path, l.description,
                   t.name as tribunal_name
            FROM listings l
            LEFT JOIN tribunals t ON t.id = l.tribunal_id
            WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def scrape_log_summary(self) -> pd.DataFrame:
        """Recent scraping activity."""
        query = """
            SELECT * FROM scrape_log ORDER BY started_at DESC LIMIT 20
        """
        with self.db.connect() as conn:
            return pd.read_sql(query, conn)

    def global_stats(self) -> dict:
        """Summary statistics for dashboard header."""
        with self.db.connect() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'upcoming' THEN 1 ELSE 0 END) as upcoming,
                    SUM(CASE WHEN status = 'past' THEN 1 ELSE 0 END) as past,
                    SUM(CASE WHEN detail_scraped = 1 THEN 1 ELSE 0 END) as with_detail,
                    SUM(CASE WHEN result_status = 'sold' THEN 1 ELSE 0 END) as sold,
                    SUM(CASE WHEN final_price IS NOT NULL AND final_price > 0 THEN 1 ELSE 0 END) as with_final_price,
                    AVG(CASE WHEN status = 'upcoming' THEN mise_a_prix ELSE NULL END) as avg_upcoming_price,
                    AVG(CASE WHEN final_price > 0 THEN final_price ELSE NULL END) as avg_final_price,
                    MIN(CASE WHEN status = 'upcoming' THEN auction_date ELSE NULL END) as next_auction_date
                FROM listings
            """).fetchone()
            return dict(row) if row else {}
