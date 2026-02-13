"""Upcoming auctions page - filterable table of properties going to auction."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import pandas as pd

from analysis.metrics import AuctionMetrics
from db.database import Database


def render():
    st.header("Encheres a venir")

    db = Database()
    metrics = AuctionMetrics(db)

    # Global stats
    stats = metrics.global_stats()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total annonces", f"{stats.get('total', 0):,}")
    col2.metric("A venir", f"{stats.get('upcoming', 0):,}")
    col3.metric("Avec details", f"{stats.get('with_detail', 0):,}")
    col4.metric(
        "Prix moyen",
        f"{stats.get('avg_upcoming_price', 0):,.0f} EUR"
        if stats.get("avg_upcoming_price")
        else "N/A",
    )

    st.divider()

    # Filters — inline in central panel
    df_all = metrics.get_upcoming_listings()

    with st.expander("Filtres", expanded=False):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            departments = sorted(df_all["department_code"].dropna().unique().tolist())
            selected_depts = st.multiselect("Departements", departments, key="up_depts")
        with fc2:
            regions = sorted(df_all["region"].dropna().unique().tolist())
            selected_regions = st.multiselect("Regions", regions, key="up_regions")
        with fc3:
            types = sorted(df_all["property_type"].dropna().unique().tolist())
            selected_types = st.multiselect("Types de bien", types, key="up_types")

        price_max_val = int(df_all["mise_a_prix"].max() or 1_000_000)
        pc1, pc2 = st.columns(2)
        with pc1:
            price_min = st.number_input("Prix min (EUR)", min_value=0, max_value=price_max_val, value=0, step=10_000, key="up_pmin")
        with pc2:
            price_max = st.number_input("Prix max (EUR)", min_value=0, max_value=price_max_val, value=price_max_val, step=10_000, key="up_pmax")
        price_range = (price_min, price_max)

    # Apply filters
    filters = {}
    if selected_depts:
        filters["department_codes"] = selected_depts
    if selected_regions:
        filters["regions"] = selected_regions
    if selected_types:
        filters["property_types"] = selected_types
    filters["min_price"] = price_range[0]
    filters["max_price"] = price_range[1]

    df = metrics.get_upcoming_listings(filters)

    if df.empty:
        st.info("Aucune annonce ne correspond aux filtres selectionnes. Lancez d'abord un scraping.")
        return

    st.subheader(f"{len(df)} annonces")

    # Display columns
    display_cols = [
        "licitor_id",
        "city",
        "department_code",
        "property_type",
        "mise_a_prix",
        "surface_m2",
        "auction_date",
        "tribunal_name",
    ]
    available = [c for c in display_cols if c in df.columns]
    df_display = df[available].copy()

    # Format
    if "mise_a_prix" in df_display.columns:
        df_display["mise_a_prix"] = df_display["mise_a_prix"].apply(
            lambda x: f"{x:,.0f} EUR" if pd.notna(x) else ""
        )
    if "surface_m2" in df_display.columns:
        df_display["surface_m2"] = df_display["surface_m2"].apply(
            lambda x: f"{x:.1f} m2" if pd.notna(x) else ""
        )

    df_display.columns = [
        "N.",
        "Ville",
        "Dept",
        "Type",
        "Mise a prix",
        "Surface",
        "Date",
        "Tribunal",
    ][: len(available)]

    st.dataframe(df_display, width="stretch", hide_index=True)

    # Detail view
    st.divider()
    st.subheader("Detail d'une annonce")
    selected_id = st.selectbox(
        "Selectionner une annonce",
        df["licitor_id"].tolist(),
        format_func=lambda x: f"N.{x} - {df[df['licitor_id'] == x].iloc[0]['city'] if not df[df['licitor_id'] == x].empty else ''}",
    )

    if selected_id:
        row = df[df["licitor_id"] == selected_id].iloc[0]
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Type:** {row.get('property_type', 'N/A')}")
            st.markdown(f"**Ville:** {row.get('city', 'N/A')} ({row.get('department_code', '')})")
            st.markdown(f"**Mise a prix:** {row.get('mise_a_prix', 'N/A'):,.0f} EUR" if pd.notna(row.get('mise_a_prix')) else "**Mise a prix:** N/A")
            st.markdown(f"**Surface:** {row.get('surface_m2', 'N/A')}")
            st.markdown(f"**Date:** {row.get('auction_date', 'N/A')}")
        with col2:
            st.markdown(f"**Tribunal:** {row.get('tribunal_name', 'N/A')}")
            st.markdown(f"**Avocat:** {row.get('lawyer_name', 'N/A')}")
            st.markdown(f"**Tel:** {row.get('lawyer_phone', 'N/A')}")
            st.markdown(f"**Adresse:** {row.get('full_address', 'N/A')}")
            if pd.notna(row.get("url_path")):
                st.markdown(f"[Voir sur Licitor](https://www.licitor.com{row['url_path']})")
