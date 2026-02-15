"""Overview dashboard - landing page with key metrics and summary charts."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from analysis.metrics import AuctionMetrics
from db.database import Database
from dashboard.chart_config import apply_theme, STATUS_COLORS, STATUS_LABELS


def render():
    st.header("Vue d'ensemble")

    db = Database()
    metrics = AuctionMetrics(db)

    # Global stats
    stats = metrics.global_stats()
    total = stats.get("total", 0)
    upcoming = stats.get("upcoming", 0)
    sold = stats.get("sold", 0)
    historical = stats.get("past", 0)

    # Last scrape info
    with db.connect() as conn:
        last_log = conn.execute(
            "SELECT started_at FROM scrape_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if last_log:
        st.caption(f"Derniere mise a jour : {last_log[0][:16] if last_log[0] else 'N/A'}")

    # ── KPI Row ──────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Encheres a venir", f"{upcoming:,}")
    c2.metric("Resultats historiques", f"{historical:,}")

    if historical > 0 and sold:
        sale_rate = sold / historical * 100
        c3.metric("Taux de vente", f"{sale_rate:.0f}%")
    else:
        c3.metric("Taux de vente", "N/A")

    # Compute median ratio from historical data
    df_hist = metrics.get_historical_listings()
    sold_df = df_hist[df_hist["result_status"] == "sold"] if not df_hist.empty else pd.DataFrame()
    if not sold_df.empty and sold_df["ratio"].notna().any():
        median_ratio = sold_df["ratio"].median()
        c4.metric("Ratio median", f"{median_ratio:.2f}x")
    else:
        c4.metric("Ratio median", "N/A")

    st.divider()

    # ── Row 2: Upcoming table + Donut chart ──────────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Prochaines encheres")
        df_upcoming = metrics.get_upcoming_listings()

        if df_upcoming.empty:
            st.info("Aucune enchere a venir. Lancez un scraping.")
        else:
            df_compact = df_upcoming.head(10)[
                [c for c in ["auction_date", "city", "department_code", "property_type", "mise_a_prix"]
                 if c in df_upcoming.columns]
            ].copy()

            if "mise_a_prix" in df_compact.columns:
                df_compact["mise_a_prix"] = df_compact["mise_a_prix"].apply(
                    lambda x: f"{x:,.0f} EUR" if pd.notna(x) else ""
                )

            df_compact.columns = ["Date", "Ville", "Dept", "Type", "Mise a prix"][: len(df_compact.columns)]
            st.dataframe(df_compact, use_container_width=True, hide_index=True, height=390)

            if len(df_upcoming) > 10:
                st.caption(f"{len(df_upcoming)} encheres au total")

    with col_right:
        st.subheader("Repartition des issues")
        if not df_hist.empty:
            df_hist["label_status"] = df_hist["result_status"].map(STATUS_LABELS).fillna(df_hist["result_status"])
            status_counts = df_hist["label_status"].value_counts().reset_index()
            status_counts.columns = ["Resultat", "Nombre"]

            fig_pie = px.pie(
                status_counts,
                values="Nombre",
                names="Resultat",
                color="Resultat",
                color_discrete_map=STATUS_COLORS,
                hole=0.4,
            )
            fig_pie.update_traces(textinfo="percent+value")
            apply_theme(fig_pie)
            fig_pie.update_layout(
                margin=dict(l=20, r=20, t=20, b=20),
                height=390,
                showlegend=True,
                legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Aucune donnee historique disponible.")

    st.divider()

    # ── Row 3: Monthly trends ────────────────────────────────────────
    if not df_hist.empty:
        df_dated = df_hist[df_hist["result_date"].notna()].copy()

        if not df_dated.empty:
            df_dated["result_month"] = df_dated["result_date"].str[:7]

            # Keep last 12 months
            months_sorted = sorted(df_dated["result_month"].unique())
            last_12 = months_sorted[-12:] if len(months_sorted) > 12 else months_sorted
            df_recent = df_dated[df_dated["result_month"].isin(last_12)]

            monthly = (
                df_recent
                .groupby("result_month")
                .agg(
                    nb_total=("licitor_id", "count"),
                    nb_sold=("result_status", lambda s: (s == "sold").sum()),
                    median_ratio=("ratio", lambda s: s.dropna().median() if s.notna().any() else None),
                )
                .reset_index()
                .sort_values("result_month")
            )

            col_v, col_r = st.columns(2)

            with col_v:
                st.subheader("Volume mensuel")
                fig_vol = go.Figure()
                fig_vol.add_trace(go.Bar(
                    x=monthly["result_month"],
                    y=monthly["nb_sold"],
                    name="Vendus",
                    marker_color="#10b981",
                ))
                fig_vol.add_trace(go.Bar(
                    x=monthly["result_month"],
                    y=monthly["nb_total"] - monthly["nb_sold"],
                    name="Non vendus",
                    marker_color="#ef4444",
                ))
                apply_theme(fig_vol)
                fig_vol.update_layout(
                    barmode="stack",
                    height=350,
                    margin=dict(l=40, r=20, t=20, b=40),
                    xaxis_title="",
                    yaxis_title="Nombre",
                )
                st.plotly_chart(fig_vol, use_container_width=True)

            with col_r:
                st.subheader("Ratio median mensuel")
                monthly_ratio = monthly[monthly["median_ratio"].notna()]
                if not monthly_ratio.empty:
                    fig_ratio = go.Figure()
                    fig_ratio.add_trace(go.Scatter(
                        x=monthly_ratio["result_month"],
                        y=monthly_ratio["median_ratio"],
                        mode="lines+markers",
                        name="Ratio median",
                        line=dict(color="#6366f1", width=2),
                        fill="tozeroy",
                        fillcolor="rgba(99, 102, 241, 0.1)",
                    ))
                    fig_ratio.add_hline(y=1.0, line_dash="dash", line_color="#94a3b8",
                                        annotation_text="1x")
                    apply_theme(fig_ratio)
                    fig_ratio.update_layout(
                        height=350,
                        margin=dict(l=40, r=20, t=20, b=40),
                        xaxis_title="",
                        yaxis_title="Ratio",
                    )
                    st.plotly_chart(fig_ratio, use_container_width=True)
                else:
                    st.info("Pas de donnees de ratio disponibles.")

    # ── Alert badge ──────────────────────────────────────────────────
    unread = db.get_unread_matches()
    if unread:
        st.info(f"{len(unread)} nouvelle(s) correspondance(s) d'alertes. Consultez l'onglet Administration > Alertes.")
