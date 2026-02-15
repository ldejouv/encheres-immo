"""Administration page - composite view with scraper, alerts, and results entry tabs."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st


def render():
    st.header("Administration")

    tab_scraper, tab_alerts, tab_results = st.tabs([
        "Scraper",
        "Alertes",
        "Saisie resultats",
    ])

    with tab_scraper:
        from dashboard.views.scraper_admin import render_scraper_tab
        render_scraper_tab()

    with tab_alerts:
        from dashboard.views.alerts import render_alerts_tab
        render_alerts_tab()

    with tab_results:
        from dashboard.views.results_entry import render_results_tab
        render_results_tab()
