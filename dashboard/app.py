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

st.sidebar.title("Encheres Immo")
st.sidebar.markdown("Analyse des ventes aux encheres immobilieres")

page = st.sidebar.radio(
    "Navigation",
    [
        "Encheres a venir",
        "Analyse historique",
        "Carte des biens",
        "Alertes",
        "Saisie resultats",
        "Administration",
    ],
)

try:
    if page == "Encheres a venir":
        from dashboard.views.upcoming import render
    elif page == "Analyse historique":
        from dashboard.views.history import render
    elif page == "Carte des biens":
        from dashboard.views.map_view import render
    elif page == "Alertes":
        from dashboard.views.alerts import render
    elif page == "Saisie resultats":
        from dashboard.views.results_entry import render
    elif page == "Administration":
        from dashboard.views.scraper_admin import render

    render()
except Exception as e:
    st.error(f"Erreur lors du chargement de la page : {e}")
    st.code(traceback.format_exc())
