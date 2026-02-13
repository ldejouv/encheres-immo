"""Alert matching engine: checks new listings against user-defined criteria."""

from __future__ import annotations

import logging

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from db.database import Database

logger = logging.getLogger(__name__)


class AlertEngine:
    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    def match_listing(self, listing: dict, alert: dict) -> bool:
        """Check if a single listing matches an alert's criteria."""
        mise_a_prix = listing.get("mise_a_prix") or 0

        if alert["min_price"] is not None and mise_a_prix < alert["min_price"]:
            return False
        if alert["max_price"] is not None and mise_a_prix > alert["max_price"]:
            return False

        if alert["department_codes"]:
            depts = [d.strip() for d in alert["department_codes"].split(",")]
            if listing.get("department_code") not in depts:
                return False

        if alert["property_types"]:
            types = [t.strip().lower() for t in alert["property_types"].split(",")]
            listing_type = (listing.get("property_type") or "").lower()
            if not any(t in listing_type for t in types):
                return False

        surface = listing.get("surface_m2") or 0
        if alert["min_surface"] is not None and surface < alert["min_surface"]:
            return False
        if alert["max_surface"] is not None and surface > alert["max_surface"]:
            return False

        if alert["regions"]:
            regions = [r.strip() for r in alert["regions"].split(",")]
            listing_region = listing.get("region") or ""
            if listing_region not in regions:
                return False

        return True

    def match_new_listings(self, licitor_ids: list[int]):
        """Run all active alerts against newly scraped listings."""
        alerts = self.db.get_active_alerts()
        if not alerts:
            return

        matched = 0
        with self.db.connect() as conn:
            for lid in licitor_ids:
                row = conn.execute(
                    """SELECT l.*, t.region
                       FROM listings l
                       LEFT JOIN tribunals t ON t.id = l.tribunal_id
                       WHERE l.licitor_id = ?""",
                    (lid,),
                ).fetchone()
                if not row:
                    continue

                listing = dict(row)
                for alert in alerts:
                    if self.match_listing(listing, alert):
                        conn.execute(
                            """INSERT OR IGNORE INTO alert_matches
                               (alert_id, listing_id) VALUES (?, ?)""",
                            (alert["id"], listing["id"]),
                        )
                        matched += 1

        if matched:
            logger.info("Alert matching: %d new matches", matched)
