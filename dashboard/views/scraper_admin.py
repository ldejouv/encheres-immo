"""Scraper administration page — launch jobs and monitor progress."""

import sys
import logging
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st

from db.database import Database
from scraper.orchestrator import ScrapingOrchestrator
from scraper.progress import read_progress, is_job_running, clear_progress, request_cancel

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)

# ── Job definitions ──────────────────────────────────────────────────

JOB_TYPE_LABELS = {
    "full": "Scraping complet",
    "incremental": "Scrape incrementiel",
    "history": "Historique des adjudications",
    "map_backfill": "Backfill mises a prix",
    "surface_backfill": "Backfill surfaces",
    "detail_backfill": "Backfill details",
}

JOBS = {
    "incremental": {
        "label": "Scrape incrementiel",
        "icon": "\U0001f504",
        "description": "Recupere les nouvelles encheres a venir et leurs details.",
    },
    "history": {
        "label": "Historique des adjudications",
        "icon": "\U0001f4dc",
        "description": "Scrape toutes les audiences passees, tribunal par tribunal.",
    },
    "map_backfill": {
        "label": "Backfill mises a prix",
        "icon": "\U0001f4b0",
        "description": "Complete les mises a prix manquantes en visitant chaque page de detail.",
    },
    "surface_backfill": {
        "label": "Backfill surfaces",
        "icon": "\U0001f4d0",
        "description": "Complete les surfaces manquantes en analysant le texte de chaque page de detail.",
    },
    "backfill": {
        "label": "Backfill details",
        "icon": "\U0001f4cb",
        "description": "Scrape les pages de detail pour les annonces qui en manquent.",
    },
}


# ── Helpers ──────────────────────────────────────────────────────────

def _launch_job(mode: str, limit: int | None = None):
    """Launch a scraper job in a background thread (works on Streamlit Cloud)."""
    clear_progress()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    db = Database()
    db.initialize()
    orchestrator = ScrapingOrchestrator()

    def _run():
        try:
            if mode == "full":
                orchestrator.run_full(detail_limit=limit or 500)
            elif mode == "incremental":
                orchestrator.run_incremental()
            elif mode == "history":
                orchestrator.run_history_backfill()
            elif mode == "map-backfill":
                orchestrator.run_map_backfill(limit=limit or 500)
            elif mode == "surface-backfill":
                orchestrator.run_surface_backfill(limit=limit or 500)
            elif mode == "backfill":
                orchestrator.run_detail_backfill(limit=limit or 100)
        except Exception:
            logging.getLogger(__name__).exception("Scraper job '%s' failed", mode)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()


def _stop_job():
    """Request graceful cancellation of the running scraper thread."""
    request_cancel()


def _get_db_stats() -> dict:
    db = Database()
    with db.connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        historical = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE result_status IS NOT NULL"
        ).fetchone()[0]
        with_map = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE mise_a_prix IS NOT NULL AND mise_a_prix > 0"
        ).fetchone()[0]
        without_detail = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE detail_scraped = 0"
        ).fetchone()[0]
        without_map_hist = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE result_status IS NOT NULL AND (mise_a_prix IS NULL OR mise_a_prix = 0)"
        ).fetchone()[0]
        with_surface = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE surface_m2 IS NOT NULL AND surface_m2 > 0"
        ).fetchone()[0]
        without_surface_hist = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE result_status IS NOT NULL AND (surface_m2 IS NULL OR surface_m2 = 0)"
        ).fetchone()[0]
        upcoming = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE status = 'upcoming'"
        ).fetchone()[0]

        # Last scrape log
        last_log = conn.execute(
            "SELECT * FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone()

    return {
        "total": total,
        "historical": historical,
        "with_map": with_map,
        "without_detail": without_detail,
        "without_map_hist": without_map_hist,
        "with_surface": with_surface,
        "without_surface_hist": without_surface_hist,
        "upcoming": upcoming,
        "last_log": dict(last_log) if last_log else None,
    }


# ── Main render ──────────────────────────────────────────────────────

def render():
    """Legacy entry point — redirects to admin.py composite page."""
    from dashboard.views.admin import render as render_admin
    render_admin()


def render_scraper_tab():
    st.subheader("Scraper")

    # ── DB Stats overview ────────────────────────────────────────────
    stats = _get_db_stats()
    st.subheader("Etat de la base de donnees")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total annonces", f"{stats['total']:,}")
    c2.metric("Encheres a venir", f"{stats['upcoming']:,}")
    c3.metric("Resultats historiques", f"{stats['historical']:,}")

    c4, c5, c6, c7 = st.columns(4)
    c4.metric("Avec mise a prix", f"{stats['with_map']:,}")
    c5.metric("Sans mise a prix (hist.)", f"{stats['without_map_hist']:,}")
    c6.metric("Avec surface", f"{stats['with_surface']:,}")
    c7.metric("Sans surface (hist.)", f"{stats['without_surface_hist']:,}")

    if stats["last_log"]:
        log = stats["last_log"]
        st.caption(
            f"Dernier scrape : **{JOB_TYPE_LABELS.get(log['scrape_type'], log['scrape_type'])}** "
            f"— {log['started_at']} — "
            f"{log['listings_new']} nouveaux, {log['listings_updated']} mis a jour, "
            f"{log['errors']} erreurs"
        )

    st.divider()

    # ── Live progress monitor ────────────────────────────────────────
    progress = read_progress()
    running = is_job_running()

    if running and progress:
        _render_progress(progress)
        st.divider()
    elif progress and progress.get("status") in ("finished", "error", "cancelled"):
        _render_progress_summary(progress)
        st.divider()

    # ── Job launcher ─────────────────────────────────────────────────
    st.subheader("Lancer un scraping")

    if running:
        st.warning("Un scraper est deja en cours d'execution. Attendez qu'il termine ou arretez-le.")
    else:
        st.markdown(
            "Scrape toutes les encheres a venir (toutes dates d'audience), "
            "puis complete les details manquants (surface, mise a prix)."
        )

        full_limit = st.number_input(
            "Limite de backfill par phase",
            min_value=10,
            max_value=20000,
            value=500,
            step=100,
            key="full_limit",
        )

        if st.button("Lancer le scraping complet", type="primary", key="btn_full"):
            _launch_job("full", limit=full_limit)
            st.rerun()

        with st.expander("Actions individuelles"):
            tabs = st.tabs([j["label"] for j in JOBS.values()])

            for tab, (key, job) in zip(tabs, JOBS.items()):
                with tab:
                    st.markdown(f"{job['icon']} **{job['label']}**")
                    st.caption(job["description"])

                    if key == "map_backfill":
                        limit = st.number_input(
                            "Nombre max d'annonces",
                            min_value=10,
                            max_value=20000,
                            value=max(min(stats["without_map_hist"], 20000), 10),
                            step=100,
                            key=f"limit_{key}",
                        )
                        if st.button(f"Lancer {job['label']}", key=f"btn_{key}"):
                            _launch_job("map-backfill", limit=limit)
                            st.rerun()

                    elif key == "surface_backfill":
                        limit = st.number_input(
                            "Nombre max d'annonces",
                            min_value=10,
                            max_value=20000,
                            value=max(min(stats["without_surface_hist"], 20000), 10),
                            step=100,
                            key=f"limit_{key}",
                        )
                        if st.button(f"Lancer {job['label']}", key=f"btn_{key}"):
                            _launch_job("surface-backfill", limit=limit)
                            st.rerun()

                    elif key == "backfill":
                        limit = st.number_input(
                            "Nombre max d'annonces",
                            min_value=10,
                            max_value=5000,
                            value=max(min(stats["without_detail"], 5000), 10),
                            step=50,
                            key=f"limit_{key}",
                        )
                        if st.button(f"Lancer {job['label']}", key=f"btn_{key}"):
                            _launch_job("backfill", limit=limit)
                            st.rerun()

                    else:
                        if st.button(f"Lancer {job['label']}", key=f"btn_{key}"):
                            _launch_job(key)
                            st.rerun()


def _render_progress(p: dict):
    """Render the live progress dashboard."""
    st.subheader("Scraper en cours")

    job_label = JOB_TYPE_LABELS.get(p["job_type"], p["job_type"])
    status_color = "\U0001f7e2" if p["status"] == "running" else "\U0001f534"

    st.markdown(f"### {status_color} {job_label}")

    # Progress bar
    pct = p.get("progress_pct", 0)
    st.progress(min(pct / 100.0, 1.0), text=f"{pct:.1f}%")

    # KPI cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Temps ecoule", p.get("elapsed_fmt", "\u2014"))
    c2.metric("Traite", f"{p['processed']:,} / {p['total']:,}")
    c3.metric("Restant", f"{p['remaining']:,}")
    c4.metric("Temps restant estime", p.get("eta_fmt", "\u2014"))

    # Secondary stats
    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Mis a jour", f"{p['updated']:,}")
    c6.metric("Non trouves", f"{p.get('not_found', 0):,}")
    c7.metric("Erreurs", f"{p['errors']:,}")
    c8.metric("Vitesse", f"{p.get('speed_per_min', 0):.0f} / min")

    if p.get("current_item"):
        st.caption(f"En cours : {p['current_item']}")

    # Stop button
    col_stop, col_refresh = st.columns([1, 1])
    with col_stop:
        if st.button("Arreter le scraper", type="secondary"):
            _stop_job()
            st.warning("Signal d'arret envoye. Le scraper s'arretera apres l'element en cours.")
            time.sleep(2)
            st.rerun()
    with col_refresh:
        if st.button("Rafraichir", type="primary"):
            st.rerun()

    # Auto-refresh hint
    st.caption("Cliquez sur 'Rafraichir' pour mettre a jour la progression.")


def _render_progress_summary(p: dict):
    """Render summary of the last completed job."""
    job_label = JOB_TYPE_LABELS.get(p["job_type"], p["job_type"])

    if p["status"] == "finished":
        st.success(f"**{job_label}** termine avec succes !")
    elif p["status"] == "cancelled":
        st.warning(f"**{job_label}** annule par l'utilisateur.")
    else:
        st.error(f"**{job_label}** termine avec une erreur : {p.get('error_message', '?')}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Duree totale", p.get("elapsed_fmt", "\u2014"))
    c2.metric("Traites", f"{p['processed']:,}")
    c3.metric("Mis a jour", f"{p['updated']:,}")
    c4.metric("Erreurs", f"{p['errors']:,}")

    if st.button("Effacer", key="clear_progress"):
        clear_progress()
        st.rerun()
