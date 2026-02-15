"""Streamlit dashboard entry point."""

import sys
from pathlib import Path

# Ensure the project root is on sys.path so that all imports resolve.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st
import traceback

st.set_page_config(
    page_title="Encheres Immo",
    page_icon="\u2696\ufe0f",
    layout="wide",
    initial_sidebar_state="expanded",
)

from db.database import Database

# ── Load custom CSS ───────────────────────────────────────────────
_css_path = Path(__file__).resolve().parent / "style.css"
if _css_path.exists():
    st.markdown(f"<style>{_css_path.read_text()}</style>", unsafe_allow_html=True)

# Auto-initialize the database on first run (creates tables if missing)
if "db_initialized" not in st.session_state:
    Database().initialize()
    st.session_state.db_initialized = True

# ── Sidebar navigation ───────────────────────────────────────────
st.sidebar.markdown(
    '<h1 style="color:#f8fafc; font-size:1.6rem; margin-bottom:0;">'
    '\u2696\ufe0f Encheres Immo</h1>'
    '<p style="color:#94a3b8; font-size:0.85rem; margin-top:4px;">'
    'Analyse des ventes aux encheres immobilieres</p>',
    unsafe_allow_html=True,
)
st.sidebar.markdown("---")

_NAV_ITEMS = {
    "Vue d'ensemble":      "\U0001f4ca",
    "Encheres a venir":    "\U0001f3e0",
    "Analyse historique":  "\U0001f4c8",
    "Administration":      "\u2699\ufe0f",
}

page = st.sidebar.radio(
    "Navigation",
    list(_NAV_ITEMS.keys()),
    format_func=lambda p: f"{_NAV_ITEMS[p]}  {p}",
)

# ── Sidebar footer: DB stats ─────────────────────────────────────
try:
    from analysis.metrics import AuctionMetrics
    _stats = AuctionMetrics().global_stats()
    st.sidebar.markdown("---")
    st.sidebar.caption(f"{_stats.get('total', 0):,} annonces en base")
    if _stats.get("upcoming"):
        st.sidebar.caption(f"{_stats['upcoming']:,} encheres a venir")
except Exception:
    pass

# ── Page routing ─────────────────────────────────────────────────
try:
    if page == "Vue d'ensemble":
        from dashboard.views.overview import render
    elif page == "Encheres a venir":
        from dashboard.views.upcoming import render
    elif page == "Analyse historique":
        from dashboard.views.history import render
    elif page == "Administration":
        from dashboard.views.admin import render

    render()
except Exception as e:
    st.error(f"Erreur lors du chargement de la page : {e}")
    st.code(traceback.format_exc())
