"""Top-level scraping workflows: incremental, history backfill, detail backfill."""

from __future__ import annotations

import logging

import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from config import AppConfig, config as app_config
from db.database import Database
from scraper.detail_scraper import DetailScraper
from scraper.history_scraper import HistoryScraper
from scraper.index_scraper import IndexScraper
from scraper.progress import ProgressWriter, _clear_cancel_flag
from scraper.tribunal_scraper import TribunalScraper

logger = logging.getLogger(__name__)


class ScrapeCancelled(Exception):
    """Raised when a user cancels a running scrape."""


class ScrapingOrchestrator:
    """Coordinates scraping workflows and triggers alert matching."""

    def __init__(self, cfg: AppConfig | None = None):
        self.cfg = cfg or app_config
        self.db = Database(self.cfg.db)
        self.index_scraper = IndexScraper(self.cfg.scraper)
        self.tribunal_scraper = TribunalScraper(self.cfg.scraper)
        self.detail_scraper = DetailScraper(self.cfg.scraper)
        self.history_scraper = HistoryScraper(self.cfg.scraper)

    def run_incremental(self):
        """Daily run: scrape upcoming auctions, get details for new ones."""
        log_id = self.db.start_scrape_log("incremental")
        errors = 0
        new_listing_ids: list[int] = []

        pw = ProgressWriter("incremental", total=1)  # will update total later

        try:
            # 1. Get tribunal list
            tribunals = self.index_scraper.scrape()
            self.db.upsert_tribunals(tribunals)
            logger.info("Found %d tribunals", len(tribunals))

            active = [t for t in tribunals if t.auction_count > 0]
            pw.total = len(active) + 1  # tribunals + detail phase

            # 2. Scrape each tribunal's listings
            for t in active:
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                try:
                    summaries = self.tribunal_scraper.scrape(t.url_path)
                    for s in summaries:
                        is_new = self.db.upsert_listing_summary(
                            s, tribunal_slug=t.slug
                        )
                        if is_new:
                            new_listing_ids.append(s.licitor_id)
                    pw.tick(updated=True, current_item=t.slug)
                except Exception as e:
                    logger.error("Error scraping tribunal %s: %s", t.slug, e)
                    errors += 1
                    pw.tick(error=True, current_item=t.slug)

            logger.info("Found %d new listings", len(new_listing_ids))

            # 3. Scrape detail pages for new listings
            if new_listing_ids:
                pw.total = pw.processed + len(new_listing_ids) + 1
            for lid in new_listing_ids:
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                listing = self.db.get_listing_by_licitor_id(lid)
                if listing and not listing["detail_scraped"]:
                    try:
                        detail = self.detail_scraper.scrape(listing["url_path"])
                        self.db.update_listing_detail(detail)
                        pw.tick(updated=True, current_item=f"detail #{lid}")
                    except Exception as e:
                        logger.error("Detail scrape failed for %d: %s", lid, e)
                        errors += 1
                        pw.tick(error=True, current_item=f"detail #{lid}")

            # 4. Mark past auctions
            self.db.mark_past_auctions()

            # 5. Run alert matching
            self._match_alerts(new_listing_ids)

            pw.finish()
        except ScrapeCancelled:
            pw.cancel()
            logger.info("Incremental scrape cancelled by user.")
        except Exception as exc:
            pw.abort(str(exc))
            raise
        finally:
            _clear_cancel_flag()
            self.db.finish_scrape_log(
                log_id,
                listings_new=len(new_listing_ids),
                errors=errors,
            )

        logger.info(
            "Incremental scrape complete: %d new, %d errors",
            len(new_listing_ids),
            errors,
        )

    def run_history_backfill(
        self,
        max_hearings_per_tribunal: int = 200,
        tribunal_slugs: list[str] | None = None,
    ):
        """Scrape historical results tribunal by tribunal."""
        log_id = self.db.start_scrape_log("history")
        new_count = 0
        updated_count = 0
        errors = 0
        total_pages = 0

        pw = ProgressWriter("history", total=1)

        try:
            # 1. Discover all tribunals with history from the index page
            tribunals = self.history_scraper.discover_tribunal_results_urls()
            logger.info("Found %d tribunals with historical data", len(tribunals))

            if tribunal_slugs:
                tribunals = [
                    t for t in tribunals if t["slug"] in tribunal_slugs
                ]
                logger.info("Filtered to %d tribunals", len(tribunals))

            pw.total = len(tribunals)

            # 2. Scrape each tribunal's history
            for i, tribunal in enumerate(tribunals, 1):
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                slug = tribunal["slug"]
                start_url = tribunal["url_path"]
                total_expected = tribunal["total_count"]

                logger.info(
                    "=== [%d/%d] Scraping history for %s (%s) — %d listings expected ===",
                    i, len(tribunals), tribunal["name"], slug, total_expected,
                )

                try:
                    summaries = self.history_scraper.scrape_tribunal_history(
                        start_url=start_url,
                        tribunal_slug=slug,
                        max_hearings=max_hearings_per_tribunal,
                    )

                    for s in summaries:
                        try:
                            is_new = self.db.upsert_listing_summary(
                                s,
                                tribunal_slug=slug,
                                is_historical=True,
                                auction_date=s.result_date,
                            )
                            if is_new:
                                new_count += 1
                            else:
                                updated_count += 1
                        except Exception as e:
                            logger.error(
                                "Insert failed for licitor_id %d: %s",
                                s.licitor_id, e,
                            )
                            errors += 1

                    total_pages += 1
                    pw.tick(updated=True, current_item=f"{tribunal['name']} ({len(summaries)})")
                    logger.info(
                        "[%s] Done: %d listings scraped", slug, len(summaries)
                    )

                except Exception as e:
                    logger.error(
                        "Failed to scrape history for %s: %s", slug, e
                    )
                    errors += 1
                    pw.tick(error=True, current_item=slug)

            pw.finish()
        except ScrapeCancelled:
            pw.cancel()
            logger.info("History backfill cancelled by user.")
        except Exception as exc:
            pw.abort(str(exc))
            raise
        finally:
            _clear_cancel_flag()
            self.db.finish_scrape_log(
                log_id,
                pages_scraped=total_pages,
                listings_new=new_count,
                listings_updated=updated_count,
                errors=errors,
            )

        logger.info(
            "History backfill complete: %d new, %d updated, %d errors",
            new_count, updated_count, errors,
        )

    def run_map_backfill(self, limit: int = 500):
        """Backfill mise_a_prix by visiting detail pages of listings that lack it."""
        log_id = self.db.start_scrape_log("map_backfill")
        updated = 0
        not_found = 0
        errors = 0

        try:
            listings = self.db.get_listings_without_mise_a_prix(limit=limit)
            logger.info("MAP backfill: %d listings to process", len(listings))

            pw = ProgressWriter("map_backfill", total=len(listings))

            for i, listing in enumerate(listings, 1):
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                lid = listing["licitor_id"]
                url = listing["url_path"]
                try:
                    mise_a_prix = self.detail_scraper.scrape_mise_a_prix(url)
                    if mise_a_prix and mise_a_prix > 0:
                        self.db.update_listing_mise_a_prix(lid, mise_a_prix)
                        updated += 1
                        pw.tick(updated=True, current_item=f"#{lid}")
                    else:
                        not_found += 1
                        pw.tick(not_found=True, current_item=f"#{lid}")

                    if i % 50 == 0:
                        logger.info(
                            "MAP backfill progress: %d/%d (updated=%d, not_found=%d, errors=%d)",
                            i, len(listings), updated, not_found, errors,
                        )
                except Exception as e:
                    logger.error("MAP backfill failed for %d (%s): %s", lid, url, e)
                    errors += 1
                    pw.tick(error=True, current_item=f"#{lid} ERREUR")

            pw.finish()
        except ScrapeCancelled:
            pw.cancel()
            logger.info("MAP backfill cancelled by user.")
        except Exception as exc:
            pw.abort(str(exc))
            raise
        finally:
            _clear_cancel_flag()
            self.db.finish_scrape_log(
                log_id,
                pages_scraped=updated + not_found + errors,
                listings_updated=updated,
                errors=errors,
                notes=f"not_found={not_found}",
            )

        logger.info(
            "MAP backfill complete: %d updated, %d not found, %d errors (of %d total)",
            updated, not_found, errors, len(listings),
        )

    def run_surface_backfill(self, limit: int = 500):
        """Backfill surface_m2 by visiting detail pages of listings that lack it."""
        log_id = self.db.start_scrape_log("surface_backfill")
        updated = 0
        not_found = 0
        errors = 0

        try:
            listings = self.db.get_listings_without_surface(limit=limit)
            logger.info("Surface backfill: %d listings to process", len(listings))

            pw = ProgressWriter("surface_backfill", total=len(listings))

            for i, listing in enumerate(listings, 1):
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                lid = listing["licitor_id"]
                url = listing["url_path"]
                try:
                    surface = self.detail_scraper.scrape_surface(url)
                    if surface and surface > 0:
                        self.db.update_listing_surface(lid, surface)
                        updated += 1
                        pw.tick(updated=True, current_item=f"#{lid}")
                    else:
                        not_found += 1
                        pw.tick(not_found=True, current_item=f"#{lid}")

                    if i % 50 == 0:
                        logger.info(
                            "Surface backfill progress: %d/%d (updated=%d, not_found=%d, errors=%d)",
                            i, len(listings), updated, not_found, errors,
                        )
                except Exception as e:
                    logger.error("Surface backfill failed for %d (%s): %s", lid, url, e)
                    errors += 1
                    pw.tick(error=True, current_item=f"#{lid} ERREUR")

            pw.finish()
        except ScrapeCancelled:
            pw.cancel()
            logger.info("Surface backfill cancelled by user.")
        except Exception as exc:
            pw.abort(str(exc))
            raise
        finally:
            _clear_cancel_flag()
            self.db.finish_scrape_log(
                log_id,
                pages_scraped=updated + not_found + errors,
                listings_updated=updated,
                errors=errors,
                notes=f"not_found={not_found}",
            )

        logger.info(
            "Surface backfill complete: %d updated, %d not found, %d errors (of %d total)",
            updated, not_found, errors, len(listings),
        )

    def run_detail_backfill(self, limit: int = 100):
        """Scrape detail pages for listings missing detail data."""
        log_id = self.db.start_scrape_log("detail_backfill")
        updated = 0
        errors = 0
        pw = ProgressWriter("detail_backfill", total=1)

        try:
            listings = self.db.get_listings_without_detail(limit=limit)
            logger.info("Detail backfill: %d listings to process", len(listings))
            pw.total = len(listings)

            for listing in listings:
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                try:
                    detail = self.detail_scraper.scrape(listing["url_path"])
                    self.db.update_listing_detail(detail)
                    updated += 1
                    pw.tick(updated=True, current_item=f"#{listing['licitor_id']}")
                except Exception as e:
                    logger.error(
                        "Detail scrape failed for %s: %s",
                        listing["licitor_id"],
                        e,
                    )
                    errors += 1
                    pw.tick(error=True, current_item=f"#{listing['licitor_id']} ERREUR")

            pw.finish()
        except ScrapeCancelled:
            pw.cancel()
            logger.info("Detail backfill cancelled by user.")
        except Exception as exc:
            pw.abort(str(exc))
            raise
        finally:
            _clear_cancel_flag()
            self.db.finish_scrape_log(
                log_id,
                listings_updated=updated,
                errors=errors,
            )

        logger.info("Detail backfill: %d updated, %d errors", updated, errors)

    def run_full(self, detail_limit: int = 500):
        """Unified process: upcoming auctions + detail/MAP/surface backfill."""
        new_listing_ids: list[int] = []
        errors = 0

        log_id = self.db.start_scrape_log("full_index")
        pw = ProgressWriter("full", total=1)

        try:
            # ── Phase 1: Index (discover tribunals) ──────────────────
            pw.set_phase("Decouverte des tribunaux", phase_number=1, phase_total=5)
            tribunals = self.index_scraper.scrape()
            self.db.upsert_tribunals(tribunals)
            logger.info("Found %d tribunals", len(tribunals))

            active = [t for t in tribunals if t.auction_count > 0]
            pw.total = len(active)

            # ── Phase 2: Scrape upcoming auctions ────────────────────
            pw.set_phase("Encheres a venir", phase_number=2, phase_total=5)
            for t in active:
                if pw.is_cancel_requested():
                    raise ScrapeCancelled()
                try:
                    summaries = self.tribunal_scraper.scrape(t.url_path)
                    for s in summaries:
                        is_new = self.db.upsert_listing_summary(
                            s, tribunal_slug=t.slug
                        )
                        if is_new:
                            new_listing_ids.append(s.licitor_id)
                    pw.tick(
                        updated=True,
                        current_item=f"{t.name} ({len(summaries)} annonces)",
                    )
                except Exception as e:
                    logger.error("Error scraping tribunal %s: %s", t.slug, e)
                    errors += 1
                    pw.tick(error=True, current_item=t.slug)

            logger.info("Found %d new listings", len(new_listing_ids))

            self.db.mark_past_auctions()
            self._match_alerts(new_listing_ids)

            # ── Phase 3: Detail backfill (includes surface + MAP) ────
            listings = self.db.get_listings_without_detail(limit=detail_limit)
            if listings:
                pw.set_phase("Backfill details", phase_number=3, phase_total=5)
                pw.total = pw.processed + len(listings)
                for listing in listings:
                    if pw.is_cancel_requested():
                        raise ScrapeCancelled()
                    try:
                        detail = self.detail_scraper.scrape(listing["url_path"])
                        self.db.update_listing_detail(detail)
                        pw.tick(updated=True, current_item=f"Annonce #{listing['licitor_id']}")
                    except Exception as e:
                        logger.error("Detail failed for %s: %s", listing["licitor_id"], e)
                        errors += 1
                        pw.tick(error=True, current_item=f"#{listing['licitor_id']}")

            # ── Phase 4: MAP backfill (historical without mise_a_prix) ─
            listings_map = self.db.get_listings_without_mise_a_prix(limit=detail_limit)
            if listings_map:
                pw.set_phase("Backfill mises a prix", phase_number=4, phase_total=5)
                pw.total = pw.processed + len(listings_map)
                for listing in listings_map:
                    if pw.is_cancel_requested():
                        raise ScrapeCancelled()
                    try:
                        map_val = self.detail_scraper.scrape_mise_a_prix(listing["url_path"])
                        if map_val and map_val > 0:
                            self.db.update_listing_mise_a_prix(listing["licitor_id"], map_val)
                            pw.tick(updated=True, current_item=f"Annonce #{listing['licitor_id']}")
                        else:
                            pw.tick(not_found=True, current_item=f"Annonce #{listing['licitor_id']}")
                    except Exception as e:
                        logger.error("MAP backfill failed for %d: %s", listing["licitor_id"], e)
                        errors += 1
                        pw.tick(error=True, current_item=f"#{listing['licitor_id']}")

            # ── Phase 5: Surface backfill (historical without surface) ─
            listings_surf = self.db.get_listings_without_surface(limit=detail_limit)
            if listings_surf:
                pw.set_phase("Backfill surfaces", phase_number=5, phase_total=5)
                pw.total = pw.processed + len(listings_surf)
                for listing in listings_surf:
                    if pw.is_cancel_requested():
                        raise ScrapeCancelled()
                    try:
                        surf = self.detail_scraper.scrape_surface(listing["url_path"])
                        if surf and surf > 0:
                            self.db.update_listing_surface(listing["licitor_id"], surf)
                            pw.tick(updated=True, current_item=f"Annonce #{listing['licitor_id']}")
                        else:
                            pw.tick(not_found=True, current_item=f"Annonce #{listing['licitor_id']}")
                    except Exception as e:
                        logger.error("Surface backfill failed for %d: %s", listing["licitor_id"], e)
                        errors += 1
                        pw.tick(error=True, current_item=f"#{listing['licitor_id']}")

            pw.finish()
        except ScrapeCancelled:
            pw.cancel()
            logger.info("Full scrape cancelled by user.")
        except Exception as exc:
            pw.abort(str(exc))
            raise
        finally:
            _clear_cancel_flag()
            self.db.finish_scrape_log(
                log_id,
                listings_new=len(new_listing_ids),
                errors=errors,
            )

        logger.info(
            "Full scrape complete: %d new, %d errors",
            len(new_listing_ids),
            errors,
        )

    def _match_alerts(self, licitor_ids: list[int]):
        """Run all active alerts against newly scraped listings."""
        from analysis.alerts import AlertEngine

        engine = AlertEngine(self.db)
        engine.match_new_listings(licitor_ids)
