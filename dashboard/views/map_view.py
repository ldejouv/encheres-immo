"""Map visualization of auction properties using Folium."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
import pandas as pd

from analysis.metrics import AuctionMetrics
from db.database import Database


def render():
    st.header("Carte des biens")

    db = Database()
    metrics = AuctionMetrics(db)
    df = metrics.geographic_data()

    if df.empty:
        st.info(
            "Aucun bien avec coordonnees GPS. "
            "Scrapez des pages de detail pour obtenir les coordonnees."
        )
        return

    # Filters in sidebar
    with st.sidebar:
        st.subheader("Filtres carte")
        status_filter = st.multiselect(
            "Statut", ["upcoming", "past"], default=["upcoming"]
        )
        departments = sorted(df["department_code"].dropna().unique().tolist())
        dept_filter = st.multiselect("Departements", departments)

        price_min, price_max = st.slider(
            "Prix (EUR)",
            min_value=0,
            max_value=int(df["mise_a_prix"].max() or 1_000_000),
            value=(0, int(df["mise_a_prix"].max() or 1_000_000)),
            step=10_000,
        )

    # Apply filters
    df_filtered = df.copy()
    if status_filter:
        df_filtered = df_filtered[df_filtered["status"].isin(status_filter)]
    if dept_filter:
        df_filtered = df_filtered[df_filtered["department_code"].isin(dept_filter)]
    df_filtered = df_filtered[
        (df_filtered["mise_a_prix"].fillna(0) >= price_min)
        & (df_filtered["mise_a_prix"].fillna(0) <= price_max)
    ]

    st.markdown(f"**{len(df_filtered)}** biens affiches sur la carte")

    # Create map centered on France
    m = folium.Map(location=[46.603354, 1.888334], zoom_start=6)
    cluster = MarkerCluster().add_to(m)

    for _, row in df_filtered.iterrows():
        color = "blue" if row["status"] == "upcoming" else "gray"
        price_text = f"{row['mise_a_prix']:,.0f} EUR" if pd.notna(row["mise_a_prix"]) else "N/A"
        surface_text = f"{row['surface_m2']:.0f} m2" if pd.notna(row["surface_m2"]) else ""

        popup_html = f"""
            <b>{row.get('property_type', 'Bien')}</b><br>
            {row.get('city', '')} ({row.get('department_code', '')})<br>
            Mise a prix: {price_text}<br>
            {f'Surface: {surface_text}<br>' if surface_text else ''}
            Date: {row.get('auction_date', 'N/A')}<br>
            <a href="https://www.licitor.com{row['url_path']}"
               target="_blank">Voir sur Licitor</a>
        """
        folium.Marker(
            [row["latitude"], row["longitude"]],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row.get('city', '')} - {price_text}",
            icon=folium.Icon(color=color, icon="home", prefix="fa"),
        ).add_to(cluster)

    st_folium(m, width=1200, height=600, returned_objects=[])
