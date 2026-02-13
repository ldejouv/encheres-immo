"""Scraper administration page — launch jobs and monitor progress."""

import sys
import subprocess
import signal
import os
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st

from db.database import Database
from scraper.progress import read_progress, is_job_running, clear_progress

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)

# ── Job definitions ──────────────────────────────────────────────────

JOBS = {
    "incremental": {
        "label": "Scrape incrementiel",
        "icon": "🔄",
        "description": "Recupere les nouvelles encheres a venir et leurs details.",
        "cmd": ["python3", "scripts/run_scrape.py", "incremental", "--log-level", "INFO"],
    },
    "history": {
        "label": "Historique des adjudications",
        "icon": "📜",
        "description": "Scrape toutes les audiences passees, tribunal par tribunal.",
        "cmd": ["python3", "scripts/run_scrape.py", "history", "--log-level", "INFO"],
    },
    "map_backfill": {
        "label": "Backfill mises a prix",
        "icon": "💰",
        "description": "Complete les mises a prix manquantes en visitant chaque page de detail.",
        "cmd_fn": lambda limit: [
            "python3", "scripts/run_scrape.py", "map-backfill",
            "--limit", str(limit), "--log-level", "INFO",
        ],
    },
    "surface_backfill": {
        "label": "Backfill surfaces",
        "icon": "📐",
        "description": "Complete les surfaces manquantes en analysant le texte de chaque page de detail.",
        "cmd_fn": lambda limit: [
            "python3", "scripts/run_scrape.py", "surface-backfill",
            "--limit", str(limit), "--log-level", "INFO",
        ],
    },
    "backfill": {
        "label": "Backfill details",
        "icon": "📋",
        "description": "Scrape les pages de detail pour les annonces qui en manquent.",
        "cmd_fn": lambda limit: [
            "python3", "scripts/run_scrape.py", "backfill",
            "--limit", str(limit), "--log-level", "INFO",
        ],
    },
}

JOB_TYPE_LABELS = {
    "incremental": "Scrape incrementiel",
    "history": "Historique des adjudications",
    "map_backfill": "Backfill mises a prix",
    "surface_backfill": "Backfill surfaces",
    "detail_backfill": "Backfill details",
}


# ── Helpers ──────────────────────────────────────────────────────────

def _launch_job(cmd: list[str]):
    """Launch a scraper job as a detached subprocess."""
    clear_progress()
    subprocess.Popen(
        cmd,
        cwd=_PROJECT_ROOT,
        stdout=open(os.path.join(_PROJECT_ROOT, "data", "scrape_stdout.log"), "w"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def _stop_job(pid: int):
    """Send SIGTERM to the scraper process."""
    try:
        os.kill(pid, signal.SIGTERM)
        return True
    except OSError:
        return False


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
    st.header("Administration du scraper")

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
    elif progress and progress.get("status") in ("finished", "error"):
        _render_progress_summary(progress)
        st.divider()

    # ── Job launcher ─────────────────────────────────────────────────
    st.subheader("Lancer un scraper")

    if running:
        st.warning("Un scraper est deja en cours d'execution. Attendez qu'il termine ou arretez-le.")
    else:
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
                    if st.button(f"Lancer {job['label']}", key=f"btn_{key}", type="primary"):
                        cmd = job["cmd_fn"](limit)
                        _launch_job(cmd)
                        st.success(f"{job['label']} lance ! Rechargez la page pour voir la progression.")
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
                    if st.button(f"Lancer {job['label']}", key=f"btn_{key}", type="primary"):
                        cmd = job["cmd_fn"](limit)
                        _launch_job(cmd)
                        st.success(f"{job['label']} lance ! Rechargez la page pour voir la progression.")
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
                    if st.button(f"Lancer {job['label']}", key=f"btn_{key}", type="primary"):
                        cmd = job["cmd_fn"](limit)
                        _launch_job(cmd)
                        st.success(f"{job['label']} lance !")
                        st.rerun()

                else:
                    if st.button(f"Lancer {job['label']}", key=f"btn_{key}", type="primary"):
                        _launch_job(job["cmd"])
                        st.success(f"{job['label']} lance !")
                        st.rerun()


def _render_progress(p: dict):
    """Render the live progress dashboard."""
    st.subheader("Scraper en cours")

    job_label = JOB_TYPE_LABELS.get(p["job_type"], p["job_type"])
    status_color = "🟢" if p["status"] == "running" else "🔴"

    st.markdown(f"### {status_color} {job_label}")

    # Progress bar
    pct = p.get("progress_pct", 0)
    st.progress(min(pct / 100.0, 1.0), text=f"{pct:.1f}%")

    # KPI cards
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Temps ecoule", p.get("elapsed_fmt", "—"))
    c2.metric("Traite", f"{p['processed']:,} / {p['total']:,}")
    c3.metric("Restant", f"{p['remaining']:,}")
    c4.metric("Temps restant estime", p.get("eta_fmt", "—"))

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
            pid = p.get("pid")
            if pid and _stop_job(pid):
                st.warning("Signal d'arret envoye.")
                clear_progress()
                st.rerun()
            else:
                st.error("Impossible d'arreter le processus.")
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
    else:
        st.error(f"**{job_label}** termine avec une erreur : {p.get('error_message', '?')}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Duree totale", p.get("elapsed_fmt", "—"))
    c2.metric("Traites", f"{p['processed']:,}")
    c3.metric("Mis a jour", f"{p['updated']:,}")
    c4.metric("Erreurs", f"{p['errors']:,}")

    if st.button("Effacer", key="clear_progress"):
        clear_progress()
        st.rerun()
