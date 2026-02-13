#!/usr/bin/env python3
"""CLI for running scraping tasks.

Usage:
    python3 scripts/run_scrape.py incremental
    python3 scripts/run_scrape.py history
    python3 scripts/run_scrape.py history --max-hearings 5
    python3 scripts/run_scrape.py history --tribunals tj-paris tj-versailles
    python3 scripts/run_scrape.py backfill --limit 50
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.database import Database
from scraper.orchestrator import ScrapingOrchestrator


def main():
    parser = argparse.ArgumentParser(description="Licitor scraping CLI")
    parser.add_argument(
        "mode",
        choices=["incremental", "history", "backfill", "map-backfill", "surface-backfill"],
        help="Scraping mode",
    )
    parser.add_argument("--limit", type=int, default=500, help="Backfill limit")
    parser.add_argument(
        "--max-hearings",
        type=int,
        default=200,
        help="Max hearing dates to scrape per tribunal (history mode)",
    )
    parser.add_argument(
        "--tribunals",
        nargs="*",
        default=None,
        help="Tribunal slugs to scrape (history mode). E.g. tj-paris tj-versailles",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Ensure DB is initialized
    db = Database()
    db.initialize()

    orchestrator = ScrapingOrchestrator()

    if args.mode == "incremental":
        orchestrator.run_incremental()
    elif args.mode == "history":
        orchestrator.run_history_backfill(
            max_hearings_per_tribunal=args.max_hearings,
            tribunal_slugs=args.tribunals,
        )
    elif args.mode == "backfill":
        orchestrator.run_detail_backfill(limit=args.limit)
    elif args.mode == "map-backfill":
        orchestrator.run_map_backfill(limit=args.limit)
    elif args.mode == "surface-backfill":
        orchestrator.run_surface_backfill(limit=args.limit)


if __name__ == "__main__":
    main()
