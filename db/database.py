from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Optional

from db.models import ListingDetail, ListingSummary, TribunalInfo

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from config import config as app_config


class Database:
    def __init__(self, db_config=None):
        cfg = db_config or app_config.db
        self.db_path = cfg.db_path
        self.schema_path = cfg.schema_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self):
        schema = self.schema_path.read_text(encoding="utf-8")
        with self.connect() as conn:
            conn.executescript(schema)
        self._migrate()

    def _migrate(self):
        """Add columns / fix constraints that may not exist in older databases."""
        column_migrations = [
            "ALTER TABLE listings ADD COLUMN result_status TEXT",
            "ALTER TABLE listings ADD COLUMN final_price INTEGER",
            "ALTER TABLE listings ADD COLUMN result_date TEXT",
        ]
        with self.connect() as conn:
            for sql in column_migrations:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError:
                    pass  # column already exists

            # Recreate scrape_log if it has an old CHECK constraint missing
            # newer scrape types.  Safe: preserves existing data.
            try:
                conn.execute(
                    "INSERT INTO scrape_log (scrape_type) VALUES ('surface_backfill')"
                )
                # If that worked, delete the test row
                conn.execute(
                    "DELETE FROM scrape_log WHERE scrape_type='surface_backfill' AND finished_at IS NULL AND pages_scraped=0"
                )
            except sqlite3.IntegrityError:
                # Constraint is outdated — rebuild the table
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS scrape_log_new (
                        id              INTEGER PRIMARY KEY AUTOINCREMENT,
                        started_at      TEXT DEFAULT (datetime('now')),
                        finished_at     TEXT,
                        scrape_type     TEXT NOT NULL CHECK(scrape_type IN (
                            'full_index', 'incremental', 'history',
                            'detail_backfill', 'map_backfill', 'surface_backfill'
                        )),
                        pages_scraped   INTEGER DEFAULT 0,
                        listings_new    INTEGER DEFAULT 0,
                        listings_updated INTEGER DEFAULT 0,
                        errors          INTEGER DEFAULT 0,
                        notes           TEXT
                    );
                    INSERT INTO scrape_log_new
                        SELECT * FROM scrape_log;
                    DROP TABLE scrape_log;
                    ALTER TABLE scrape_log_new RENAME TO scrape_log;
                """)

    # ------------------------------------------------------------------
    # Tribunals
    # ------------------------------------------------------------------

    def upsert_tribunals(self, tribunals: list[TribunalInfo]):
        with self.connect() as conn:
            for t in tribunals:
                conn.execute(
                    """INSERT INTO tribunals (name, slug, region)
                       VALUES (?, ?, ?)
                       ON CONFLICT(slug) DO UPDATE SET
                           name = excluded.name,
                           region = excluded.region""",
                    (t.name, t.slug, t.region),
                )

    def get_tribunal_id(self, conn, slug: str) -> Optional[int]:
        row = conn.execute(
            "SELECT id FROM tribunals WHERE slug = ?", (slug,)
        ).fetchone()
        return row["id"] if row else None

    # ------------------------------------------------------------------
    # Listings
    # ------------------------------------------------------------------

    def upsert_listing_summary(
        self,
        summary: ListingSummary,
        tribunal_slug: Optional[str] = None,
        is_historical: bool = False,
        auction_date: Optional[str] = None,
    ) -> bool:
        """Insert or update a listing summary. Returns True if newly inserted."""
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM listings WHERE licitor_id = ?",
                (summary.licitor_id,),
            ).fetchone()

            if existing:
                # Update with result data if available
                updates = ["last_scraped_at = datetime('now')"]
                params: list = []
                if summary.final_price is not None:
                    updates.append("final_price = ?")
                    params.append(summary.final_price)
                if summary.result_status:
                    updates.append("result_status = ?")
                    params.append(summary.result_status)
                    updates.append("status = 'past'")
                if summary.result_date:
                    updates.append("result_date = ?")
                    params.append(summary.result_date)
                if is_historical:
                    updates.append("is_historical = 1")
                params.append(summary.licitor_id)
                conn.execute(
                    f"UPDATE listings SET {', '.join(updates)} WHERE licitor_id = ?",
                    params,
                )
                return False

            tribunal_id = self.get_tribunal_id(conn, tribunal_slug) if tribunal_slug else None

            conn.execute(
                """INSERT INTO listings (
                       licitor_id, url_path, property_type,
                       department_code, city, mise_a_prix,
                       description, tribunal_id, is_historical, status,
                       auction_date, final_price, result_status, result_date
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    summary.licitor_id,
                    summary.url_path,
                    summary.property_type,
                    summary.department_code,
                    summary.city,
                    summary.mise_a_prix,
                    summary.description_short,
                    tribunal_id,
                    1 if is_historical else 0,
                    "past" if is_historical else "upcoming",
                    auction_date,
                    summary.final_price,
                    summary.result_status,
                    summary.result_date,
                ),
            )
            return True

    def update_listing_detail(self, detail: ListingDetail):
        with self.connect() as conn:
            conn.execute(
                """UPDATE listings SET
                       description = ?, surface_m2 = ?, energy_rating = ?,
                       occupancy_status = ?, full_address = ?,
                       latitude = ?, longitude = ?, cadastral_ref = ?,
                       auction_date = ?, auction_time = ?,
                       mise_a_prix = COALESCE(?, mise_a_prix),
                       case_reference = ?, has_price_reduction = ?,
                       lawyer_name = ?, lawyer_phone = ?,
                       visit_date = ?,
                       price_per_m2_min = ?, price_per_m2_avg = ?,
                       price_per_m2_max = ?,
                       view_count = ?, favorites_count = ?,
                       publication_date = ?,
                       detail_scraped = 1, last_scraped_at = datetime('now')
                   WHERE licitor_id = ?""",
                (
                    detail.description,
                    detail.surface_m2,
                    detail.energy_rating,
                    detail.occupancy_status,
                    detail.full_address,
                    detail.latitude,
                    detail.longitude,
                    detail.cadastral_ref,
                    str(detail.auction_date) if detail.auction_date else None,
                    str(detail.auction_time) if detail.auction_time else None,
                    detail.mise_a_prix,
                    detail.case_reference,
                    detail.has_price_reduction,
                    detail.lawyer_name,
                    detail.lawyer_phone,
                    detail.visit_date,
                    detail.price_per_m2_min,
                    detail.price_per_m2_avg,
                    detail.price_per_m2_max,
                    detail.view_count,
                    detail.favorites_count,
                    detail.publication_date,
                    detail.licitor_id,
                ),
            )

    def mark_past_auctions(self):
        with self.connect() as conn:
            conn.execute(
                """UPDATE listings SET status = 'past'
                   WHERE status = 'upcoming' AND auction_date < date('now')"""
            )

    def get_listings_without_detail(self, limit: int = 100) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT licitor_id, url_path FROM listings
                   WHERE detail_scraped = 0
                   ORDER BY auction_date ASC NULLS LAST
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_listing_by_licitor_id(self, licitor_id: int) -> Optional[dict]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM listings WHERE licitor_id = ?", (licitor_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_listings_without_mise_a_prix(self, limit: int = 500) -> list[dict]:
        """Get historical listings that have result data but no mise_a_prix."""
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT licitor_id, url_path FROM listings
                   WHERE result_status IS NOT NULL
                     AND (mise_a_prix IS NULL OR mise_a_prix = 0)
                   ORDER BY licitor_id DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_listing_mise_a_prix(self, licitor_id: int, mise_a_prix: int):
        """Update only the mise_a_prix for a listing."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE listings SET mise_a_prix = ? WHERE licitor_id = ?",
                (mise_a_prix, licitor_id),
            )

    def get_listings_without_surface(self, limit: int = 500) -> list[dict]:
        """Get listings that have result data but no surface_m2."""
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT licitor_id, url_path FROM listings
                   WHERE result_status IS NOT NULL
                     AND (surface_m2 IS NULL OR surface_m2 = 0)
                   ORDER BY licitor_id DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def update_listing_surface(self, licitor_id: int, surface_m2: float):
        """Update only the surface_m2 for a listing."""
        with self.connect() as conn:
            conn.execute(
                "UPDATE listings SET surface_m2 = ? WHERE licitor_id = ?",
                (surface_m2, licitor_id),
            )

    # ------------------------------------------------------------------
    # Scrape log
    # ------------------------------------------------------------------

    def start_scrape_log(self, scrape_type: str) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                "INSERT INTO scrape_log (scrape_type) VALUES (?)", (scrape_type,)
            )
            return cursor.lastrowid

    def finish_scrape_log(
        self,
        log_id: int,
        pages_scraped: int = 0,
        listings_new: int = 0,
        listings_updated: int = 0,
        errors: int = 0,
        notes: str = "",
    ):
        with self.connect() as conn:
            conn.execute(
                """UPDATE scrape_log SET
                       finished_at = datetime('now'),
                       pages_scraped = ?, listings_new = ?,
                       listings_updated = ?, errors = ?, notes = ?
                   WHERE id = ?""",
                (pages_scraped, listings_new, listings_updated, errors, notes, log_id),
            )

    # ------------------------------------------------------------------
    # Alerts
    # ------------------------------------------------------------------

    def get_active_alerts(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts WHERE is_active = 1"
            ).fetchall()
            return [dict(r) for r in rows]

    def create_alert(self, data: dict) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """INSERT INTO alerts (
                       name, min_price, max_price, department_codes,
                       regions, property_types, min_surface, max_surface,
                       tribunal_slugs
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    data["name"],
                    data.get("min_price"),
                    data.get("max_price"),
                    data.get("department_codes"),
                    data.get("regions"),
                    data.get("property_types"),
                    data.get("min_surface"),
                    data.get("max_surface"),
                    data.get("tribunal_slugs"),
                ),
            )
            return cursor.lastrowid

    def delete_alert(self, alert_id: int):
        with self.connect() as conn:
            conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))

    def toggle_alert(self, alert_id: int):
        with self.connect() as conn:
            conn.execute(
                "UPDATE alerts SET is_active = 1 - is_active, updated_at = datetime('now') WHERE id = ?",
                (alert_id,),
            )

    def insert_alert_match(self, alert_id: int, listing_id: int):
        with self.connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO alert_matches (alert_id, listing_id)
                   VALUES (?, ?)""",
                (alert_id, listing_id),
            )

    def get_unread_matches(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT am.id as match_id, a.name as alert_name,
                          l.licitor_id, l.city, l.property_type,
                          l.mise_a_prix, l.auction_date, l.url_path
                   FROM alert_matches am
                   JOIN alerts a ON a.id = am.alert_id
                   JOIN listings l ON l.id = am.listing_id
                   WHERE am.is_seen = 0
                   ORDER BY am.matched_at DESC"""
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_matches_seen(self, match_ids: list[int]):
        if not match_ids:
            return
        with self.connect() as conn:
            placeholders = ",".join("?" * len(match_ids))
            conn.execute(
                f"UPDATE alert_matches SET is_seen = 1 WHERE id IN ({placeholders})",
                match_ids,
            )

    # ------------------------------------------------------------------
    # Adjudication results
    # ------------------------------------------------------------------

    def insert_adjudication_result(self, listing_id: int, final_price: int,
                                   price_source: str = "manual", notes: str = ""):
        with self.connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO adjudication_results
                       (listing_id, final_price, price_source, notes)
                   VALUES (?, ?, ?, ?)""",
                (listing_id, final_price, price_source, notes),
            )
