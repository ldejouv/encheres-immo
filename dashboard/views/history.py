"""Historical analysis page - explorer with filters, sorting, and data visualization."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, date

from analysis.metrics import AuctionMetrics
from db.database import Database


# ── Helpers ──────────────────────────────────────────────────────────

STATUS_LABELS = {
    "sold": "Vendu",
    "carence": "Carence d'encheres",
    "non_requise": "Vente non requise",
}

COLOR_MAP = {
    "Vendu": "#2ecc71",
    "Carence d'encheres": "#e74c3c",
    "Vente non requise": "#95a5a6",
}


def _fmt_eur(x):
    return f"{x:,.0f}" if pd.notna(x) and x else ""


def _fmt_ratio(x):
    return f"{x:.2f}x" if pd.notna(x) else ""


# ── Main ─────────────────────────────────────────────────────────────

def render():
    st.header("Analyse historique des adjudications")

    db = Database()
    metrics = AuctionMetrics(db)

    # Load filter options
    opts = metrics.get_historical_filter_options()

    if not opts["result_statuses"]:
        st.info(
            "Aucune donnee historique. Lancez le scraping : "
            "`python3 scripts/run_scrape.py history`"
        )
        return

    # ── Filters — in the central panel ────────────────────────────────
    with st.expander("Filtres", expanded=True):
        # Row 1: categorical filters
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            sel_statuses = st.multiselect(
                "Resultat",
                opts["result_statuses"],
                format_func=lambda s: STATUS_LABELS.get(s, s),
                key="hist_statuses",
            )
        with fc2:
            sel_depts = st.multiselect("Departements", opts["departments"], key="hist_depts")
        with fc3:
            sel_types = st.multiselect("Types de bien", opts["property_types"], key="hist_types")
        with fc4:
            sel_tribunals = st.multiselect("Tribunaux", opts["tribunal_names"], key="hist_tribunals")

        fc5, fc6 = st.columns(2)
        with fc5:
            sel_regions = st.multiselect("Regions", opts["regions"], key="hist_regions")
        with fc6:
            city_search = st.text_input(
                "Recherche ville",
                placeholder="ex: Paris, Versailles",
                key="hist_city",
            )

        st.markdown("---")

        # Row 2: price filters with manual input
        st.markdown("**Mise a prix (EUR)**")
        _max_map = max(int(opts["max_mise_a_prix"]), 100_000)
        pm1, pm2 = st.columns(2)
        with pm1:
            map_min = st.number_input("Min", min_value=0, max_value=_max_map, value=0, step=10_000, key="hist_map_min")
        with pm2:
            map_max = st.number_input("Max", min_value=0, max_value=_max_map, value=_max_map, step=10_000, key="hist_map_max")

        if opts["max_final_price"] and opts["max_final_price"] > 0:
            st.markdown("**Prix final (EUR)**")
            _max_final = max(int(opts["max_final_price"]), 100_000)
            pf1, pf2 = st.columns(2)
            with pf1:
                final_min = st.number_input("Min", min_value=0, max_value=_max_final, value=0, step=10_000, key="hist_final_min")
            with pf2:
                final_max = st.number_input("Max", min_value=0, max_value=_max_final, value=_max_final, step=10_000, key="hist_final_max")
            final_range = (final_min, final_max)
        else:
            final_range = None

        # Prix / m2 filter
        st.markdown("**Prix / m2 (EUR)**")
        pm2_1, pm2_2 = st.columns(2)
        with pm2_1:
            prix_m2_min = st.number_input("Min", min_value=0, value=0, step=100, key="hist_pm2_min")
        with pm2_2:
            prix_m2_max = st.number_input("Max", min_value=0, value=0, step=100, key="hist_pm2_max",
                                          help="Laisser a 0 pour ne pas filtrer")

        # Surface filter
        _max_surf = opts.get("max_surface") or 0
        if _max_surf > 0:
            st.markdown("**Surface (m2)**")
            sf1, sf2 = st.columns(2)
            with sf1:
                surf_min = st.number_input("Min", min_value=0, max_value=max(int(_max_surf), 100), value=0, step=10, key="hist_surf_min")
            with sf2:
                surf_max = st.number_input("Max", min_value=0, max_value=max(int(_max_surf), 100), value=max(int(_max_surf), 100), step=10, key="hist_surf_max")
            surface_range = (surf_min, surf_max)
        else:
            surface_range = None

        st.markdown("---")

        # Date range filter
        st.markdown("**Plage de dates**")
        dt1, dt2 = st.columns(2)
        with dt1:
            date_from = st.date_input("Du", value=None, key="hist_date_from")
        with dt2:
            date_to = st.date_input("Au", value=None, key="hist_date_to")

        st.markdown("---")

        # Exclusion checkboxes
        st.markdown("**Exclure les annonces avec donnees manquantes**")
        ex1, ex2, ex3, ex4 = st.columns(4)
        with ex1:
            excl_no_surface = st.checkbox("Sans surface", key="hist_excl_surf")
        with ex2:
            excl_no_final = st.checkbox("Sans prix final", key="hist_excl_final")
        with ex3:
            excl_no_price = st.checkbox("Sans mise a prix", key="hist_excl_map")
        with ex4:
            excl_no_pm2 = st.checkbox("Sans prix/m2", key="hist_excl_pm2")

    # ── Build filters ─────────────────────────────────────────────────
    filters = {}
    if sel_statuses:
        filters["result_statuses"] = sel_statuses
    if sel_depts:
        filters["department_codes"] = sel_depts
    if sel_types:
        filters["property_types"] = sel_types
    if sel_tribunals:
        filters["tribunal_names"] = sel_tribunals
    if sel_regions:
        filters["regions"] = sel_regions
    if map_min > 0:
        filters["min_price"] = map_min
    if map_max < _max_map:
        filters["max_price"] = map_max
    if final_range and final_range[0] > 0:
        filters["min_final"] = final_range[0]
    if final_range and final_range[1] < max(int(opts["max_final_price"]), 100_000):
        filters["max_final"] = final_range[1]
    if surface_range and surface_range[0] > 0:
        filters["min_surface"] = surface_range[0]
    if surface_range and _max_surf and surface_range[1] < int(_max_surf):
        filters["max_surface"] = surface_range[1]
    if city_search.strip():
        filters["cities"] = [c.strip() for c in city_search.split(",") if c.strip()]

    # ── Load data ─────────────────────────────────────────────────────
    df = metrics.get_historical_listings(filters)

    if df.empty:
        st.warning("Aucun resultat ne correspond aux filtres selectionnes.")
        return

    # Derived columns
    df["label_status"] = df["result_status"].map(STATUS_LABELS).fillna(df["result_status"])
    df["prix_m2"] = np.where(
        (df["final_price"] > 0) & (df["surface_m2"] > 0),
        df["final_price"] / df["surface_m2"],
        np.nan,
    )

    # ── Apply client-side filters (prix/m2, dates, exclusions) ────────
    if prix_m2_min > 0:
        df = df[(df["prix_m2"].isna()) | (df["prix_m2"] >= prix_m2_min)]
    if prix_m2_max > 0:
        df = df[(df["prix_m2"].isna()) | (df["prix_m2"] <= prix_m2_max)]

    if date_from:
        df = df[(df["result_date"].isna()) | (df["result_date"] >= str(date_from))]
    if date_to:
        df = df[(df["result_date"].isna()) | (df["result_date"] <= str(date_to))]

    if excl_no_surface:
        df = df[df["surface_m2"].notna() & (df["surface_m2"] > 0)]
    if excl_no_final:
        df = df[df["final_price"].notna() & (df["final_price"] > 0)]
    if excl_no_price:
        df = df[df["mise_a_prix"].notna() & (df["mise_a_prix"] > 0)]
    if excl_no_pm2:
        df = df[df["prix_m2"].notna() & (df["prix_m2"] > 0)]

    if df.empty:
        st.warning("Aucun resultat apres application des filtres avances.")
        return

    # ── KPIs ──────────────────────────────────────────────────────────
    total = len(df)
    sold_df = df[df["result_status"] == "sold"]
    sold_count = len(sold_df)
    carence_count = len(df[df["result_status"] == "carence"])
    non_req_count = len(df[df["result_status"] == "non_requise"])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total resultats", f"{total:,}")
    c2.metric("Vendus", f"{sold_count:,}")
    c3.metric("Carences", f"{carence_count:,}")
    c4.metric("Non requises", f"{non_req_count:,}")
    if not sold_df.empty and sold_df["final_price"].notna().any():
        avg_final = sold_df["final_price"].mean()
        c5.metric("Prix final moyen", f"{avg_final:,.0f} EUR")
    else:
        c5.metric("Prix final moyen", "N/A")

    if not sold_df.empty and sold_df["ratio"].notna().any():
        c6, c7, c8 = st.columns(3)
        c6.metric("Ratio moyen (final/MAP)", f"{sold_df['ratio'].mean():.2f}x")
        c7.metric("Ratio median", f"{sold_df['ratio'].median():.2f}x")
        c8.metric("Taux de vente", f"{sold_count / total * 100:.0f}%")

    st.divider()

    # ── Tabs ──────────────────────────────────────────────────────────
    tab_table, tab_viz, tab_ratios, tab_trends, tab_geo = st.tabs([
        "Tableau des resultats",
        "Visualisations",
        "Analyse des ratios",
        "Tendances temporelles",
        "Analyse geographique",
    ])

    # ================================================================
    # TAB 1 : Data table (sorted via column headers)
    # ================================================================
    with tab_table:
        st.subheader("Explorer les resultats d'adjudications")

        # Build Licitor link column
        df_tab = df.copy()
        df_tab["licitor_url"] = df_tab["url_path"].apply(
            lambda p: f"https://www.licitor.com{p}" if pd.notna(p) and p else None
        )

        # Format for display
        display_cols = [
            "licitor_id", "city", "department_code", "property_type",
            "surface_m2", "mise_a_prix", "final_price", "prix_m2",
            "ratio", "label_status",
            "result_date", "tribunal_name", "licitor_url",
        ]
        available = [c for c in display_cols if c in df_tab.columns]
        df_show = df_tab[available].copy()

        col_names = {
            "licitor_id": "N.",
            "city": "Ville",
            "department_code": "Dept",
            "property_type": "Type",
            "surface_m2": "Surface (m2)",
            "mise_a_prix": "Mise a prix",
            "final_price": "Prix final",
            "prix_m2": "Prix/m2",
            "ratio": "Ratio",
            "label_status": "Resultat",
            "result_date": "Date",
            "tribunal_name": "Tribunal",
            "licitor_url": "Lien Licitor",
        }
        df_show = df_show.rename(columns=col_names)

        # Dynamic height: ~35px per row + header, min 400, enough for 100 rows
        _row_height = 35
        _table_height = max(400, min(len(df_show), 100) * _row_height + 40)

        st.dataframe(
            df_show,
            use_container_width=True,
            hide_index=True,
            height=_table_height,
            column_config={
                "Surface (m2)": st.column_config.NumberColumn(
                    "Surface (m2)",
                    format="%.1f m2",
                    help="Surface du bien en metres carres",
                ),
                "Mise a prix": st.column_config.NumberColumn(
                    "Mise a prix",
                    format="euro",
                    help="Mise a prix de depart",
                ),
                "Prix final": st.column_config.NumberColumn(
                    "Prix final",
                    format="euro",
                    help="Prix d'adjudication",
                ),
                "Prix/m2": st.column_config.NumberColumn(
                    "Prix/m2",
                    format="%.0f EUR/m2",
                    help="Prix d'adjudication divise par la surface",
                ),
                "Ratio": st.column_config.NumberColumn(
                    "Ratio",
                    format="%.2f x",
                    help="Prix final / Mise a prix",
                ),
                "Lien Licitor": st.column_config.LinkColumn(
                    "Lien Licitor",
                    display_text="Voir",
                    help="Ouvrir l'annonce sur Licitor.com",
                ),
            },
        )

        st.caption(f"{len(df_show)} resultats affiches sur {total} au total")

    # ================================================================
    # TAB 2 : Visualisations
    # ================================================================
    with tab_viz:
        st.subheader("Visualisations des donnees historiques")

        # ── Pie chart : repartition des issues ────────────────────────
        status_counts = df["label_status"].value_counts().reset_index()
        status_counts.columns = ["Resultat", "Nombre"]
        fig_pie = px.pie(
            status_counts,
            values="Nombre",
            names="Resultat",
            title="Repartition des issues d'encheres",
            color="Resultat",
            color_discrete_map=COLOR_MAP,
            hole=0.35,
        )
        fig_pie.update_traces(textinfo="percent+value")
        st.plotly_chart(fig_pie, use_container_width=True)

        # ── Bar chart : nb de ventes par departement ──────────────────
        if df["department_code"].notna().any():
            dept_counts = (
                df[df["department_code"].notna()]
                .groupby("department_code")
                .agg(
                    total=("licitor_id", "count"),
                    vendus=("result_status", lambda s: (s == "sold").sum()),
                    prix_final_moyen=("final_price", lambda s: s.dropna().mean() if s.notna().any() else None),
                )
                .reset_index()
                .sort_values("total", ascending=False)
                .head(25)
            )

            fig_dept = go.Figure()
            fig_dept.add_trace(go.Bar(
                x=dept_counts["department_code"],
                y=dept_counts["vendus"],
                name="Vendus",
                marker_color="#2ecc71",
            ))
            fig_dept.add_trace(go.Bar(
                x=dept_counts["department_code"],
                y=dept_counts["total"] - dept_counts["vendus"],
                name="Non vendus",
                marker_color="#e74c3c",
            ))
            fig_dept.update_layout(
                barmode="stack",
                title="Resultats par departement (top 25)",
                xaxis_title="Departement",
                yaxis_title="Nombre",
            )
            st.plotly_chart(fig_dept, use_container_width=True)

        # ── Histogram : distribution des prix finaux ──────────────────
        df_with_price = sold_df[sold_df["final_price"].notna() & (sold_df["final_price"] > 0)]
        if not df_with_price.empty:
            max_hist = st.slider(
                "Prix max pour l'histogramme (EUR)",
                50_000, 2_000_000,
                min(int(df_with_price["final_price"].quantile(0.95)), 1_000_000),
                step=50_000,
                key="hist_price_max",
            )
            df_hist = df_with_price[df_with_price["final_price"] <= max_hist]

            fig_hist = px.histogram(
                df_hist,
                x="final_price",
                nbins=40,
                title="Distribution des prix finaux d'adjudication",
                labels={"final_price": "Prix final (EUR)"},
                color_discrete_sequence=["#3498db"],
            )
            fig_hist.update_layout(yaxis_title="Nombre de biens")
            st.plotly_chart(fig_hist, use_container_width=True)

        # ── Box plot : prix final par type de bien ────────────────────
        if not df_with_price.empty and df_with_price["property_type"].notna().any():
            top_types = df_with_price["property_type"].value_counts().head(8).index.tolist()
            df_box = df_with_price[df_with_price["property_type"].isin(top_types)]
            if not df_box.empty:
                fig_box = px.box(
                    df_box,
                    x="property_type",
                    y="final_price",
                    title="Prix final par type de bien (top 8 types)",
                    labels={"property_type": "Type de bien", "final_price": "Prix final (EUR)"},
                    color="property_type",
                )
                fig_box.update_layout(showlegend=False)
                st.plotly_chart(fig_box, use_container_width=True)

        # ── Violin plot : mise a prix par statut ──────────────────────
        df_violin = df[df["mise_a_prix"].notna() & (df["mise_a_prix"] > 0)].copy()
        if not df_violin.empty:
            fig_violin = px.violin(
                df_violin,
                x="label_status",
                y="mise_a_prix",
                color="label_status",
                box=True,
                title="Distribution des mises a prix par resultat",
                labels={"label_status": "Resultat", "mise_a_prix": "Mise a prix (EUR)"},
                color_discrete_map=COLOR_MAP,
            )
            fig_violin.update_layout(showlegend=False)
            st.plotly_chart(fig_violin, use_container_width=True)

    # ================================================================
    # TAB 3 : Ratio analysis
    # ================================================================
    with tab_ratios:
        st.subheader("Analyse des ratios prix final / mise a prix")
        st.markdown(
            "Le **ratio** = prix final / mise a prix. "
            "Un ratio > 1.0 signifie une vente au-dessus de la mise a prix."
        )

        df_ratio = sold_df[sold_df["ratio"].notna()].copy()

        if df_ratio.empty:
            st.info("Pas de donnees avec mise a prix ET prix final pour calculer un ratio.")
        else:
            # KPIs ratio
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Biens avec ratio", f"{len(df_ratio):,}")
            c2.metric("Ratio moyen", f"{df_ratio['ratio'].mean():.2f}x")
            c3.metric("Ratio median", f"{df_ratio['ratio'].median():.2f}x")
            below = len(df_ratio[df_ratio["ratio"] < 1.0])
            c4.metric("Vendus sous MAP", f"{below:,} ({below / len(df_ratio) * 100:.0f}%)")

            # Histogram of ratios
            fig_r = px.histogram(
                df_ratio,
                x="ratio",
                nbins=40,
                title="Distribution des ratios prix final / mise a prix",
                labels={"ratio": "Ratio"},
                color_discrete_sequence=["#e67e22"],
            )
            fig_r.add_vline(x=1.0, line_dash="dash", line_color="gray",
                            annotation_text="1x (= mise a prix)")
            median_val = df_ratio["ratio"].median()
            fig_r.add_vline(x=median_val, line_dash="dot", line_color="blue",
                            annotation_text=f"Median {median_val:.2f}x")
            fig_r.update_layout(yaxis_title="Nombre de biens")
            st.plotly_chart(fig_r, use_container_width=True)

            # Scatter : MAP vs prix final
            fig_scatter = px.scatter(
                df_ratio,
                x="mise_a_prix",
                y="final_price",
                color="department_code",
                hover_data=["licitor_id", "city", "property_type", "ratio"],
                title="Mise a prix vs Prix final",
                labels={
                    "mise_a_prix": "Mise a prix (EUR)",
                    "final_price": "Prix final (EUR)",
                },
                opacity=0.7,
            )
            max_val = max(df_ratio["mise_a_prix"].max(), df_ratio["final_price"].max())
            fig_scatter.add_shape(
                type="line", x0=0, y0=0, x1=max_val, y1=max_val,
                line=dict(dash="dash", color="rgba(100,100,100,0.5)"),
            )
            fig_scatter.update_layout(
                legend_title="Departement",
                height=550,
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

            # Ratio by property type (boxplot)
            if df_ratio["property_type"].notna().any():
                top_t = df_ratio["property_type"].value_counts().head(8).index.tolist()
                df_rbox = df_ratio[df_ratio["property_type"].isin(top_t)]
                if not df_rbox.empty:
                    fig_rbox = px.box(
                        df_rbox,
                        x="property_type",
                        y="ratio",
                        title="Ratio par type de bien",
                        labels={"property_type": "Type", "ratio": "Ratio"},
                        color="property_type",
                    )
                    fig_rbox.add_hline(y=1.0, line_dash="dash", line_color="gray")
                    fig_rbox.update_layout(showlegend=False)
                    st.plotly_chart(fig_rbox, use_container_width=True)

            # Top gains / bargains
            st.subheader("Top encheres (plus forte hausse vs MAP)")
            st.dataframe(
                df_ratio.nlargest(10, "ratio")[
                    ["licitor_id", "city", "department_code", "property_type",
                     "mise_a_prix", "final_price", "ratio", "result_date"]
                ].rename(columns={
                    "licitor_id": "N.", "city": "Ville", "department_code": "Dept",
                    "property_type": "Type", "mise_a_prix": "MAP",
                    "final_price": "Prix final", "ratio": "Ratio",
                    "result_date": "Date",
                }),
                use_container_width=True, hide_index=True,
            )

            st.subheader("Biens vendus les moins chers (ratio le plus bas)")
            st.dataframe(
                df_ratio.nsmallest(10, "ratio")[
                    ["licitor_id", "city", "department_code", "property_type",
                     "mise_a_prix", "final_price", "ratio", "result_date"]
                ].rename(columns={
                    "licitor_id": "N.", "city": "Ville", "department_code": "Dept",
                    "property_type": "Type", "mise_a_prix": "MAP",
                    "final_price": "Prix final", "ratio": "Ratio",
                    "result_date": "Date",
                }),
                use_container_width=True, hide_index=True,
            )

    # ================================================================
    # TAB 4 : Temporal trends
    # ================================================================
    with tab_trends:
        st.subheader("Tendances temporelles des adjudications")

        df_dated = df[df["result_date"].notna()].copy()

        if df_dated.empty:
            st.info("Pas de donnees avec date de resultat.")
        else:
            df_dated["result_month"] = df_dated["result_date"].str[:7]

            # Aggregate by month
            monthly = (
                df_dated
                .groupby("result_month")
                .agg(
                    nb_total=("licitor_id", "count"),
                    nb_sold=("result_status", lambda s: (s == "sold").sum()),
                    nb_carence=("result_status", lambda s: (s == "carence").sum()),
                    nb_non_req=("result_status", lambda s: (s == "non_requise").sum()),
                    avg_final=("final_price", lambda s: s.dropna().mean() if s.notna().any() else None),
                    avg_map=("mise_a_prix", lambda s: s.dropna().mean() if s.notna().any() else None),
                    median_ratio=("ratio", lambda s: s.dropna().median() if s.notna().any() else None),
                )
                .reset_index()
                .sort_values("result_month")
            )

            # Volume bar chart
            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(
                x=monthly["result_month"], y=monthly["nb_sold"],
                name="Vendus", marker_color="#2ecc71",
            ))
            fig_vol.add_trace(go.Bar(
                x=monthly["result_month"], y=monthly["nb_carence"],
                name="Carences", marker_color="#e74c3c",
            ))
            fig_vol.add_trace(go.Bar(
                x=monthly["result_month"], y=monthly["nb_non_req"],
                name="Non requises", marker_color="#95a5a6",
            ))
            fig_vol.update_layout(
                barmode="stack",
                title="Volume des encheres par mois",
                xaxis_title="Mois",
                yaxis_title="Nombre",
            )
            st.plotly_chart(fig_vol, use_container_width=True)

            # Price evolution
            monthly_with_price = monthly[monthly["avg_final"].notna()]
            if not monthly_with_price.empty:
                fig_price = go.Figure()
                fig_price.add_trace(go.Scatter(
                    x=monthly_with_price["result_month"],
                    y=monthly_with_price["avg_final"],
                    mode="lines+markers",
                    name="Prix final moyen",
                    line=dict(color="#e67e22", width=2),
                ))
                fig_price.add_trace(go.Scatter(
                    x=monthly_with_price["result_month"],
                    y=monthly_with_price["avg_map"],
                    mode="lines+markers",
                    name="Mise a prix moyenne",
                    line=dict(color="#3498db", width=2, dash="dash"),
                ))
                fig_price.update_layout(
                    title="Evolution des prix moyens",
                    xaxis_title="Mois",
                    yaxis_title="EUR",
                )
                st.plotly_chart(fig_price, use_container_width=True)

            # Ratio evolution
            monthly_with_ratio = monthly[monthly["median_ratio"].notna()]
            if not monthly_with_ratio.empty:
                fig_ratio_t = go.Figure()
                fig_ratio_t.add_trace(go.Scatter(
                    x=monthly_with_ratio["result_month"],
                    y=monthly_with_ratio["median_ratio"],
                    mode="lines+markers",
                    name="Ratio median",
                    line=dict(color="#9b59b6", width=2),
                    fill="tozeroy",
                    fillcolor="rgba(155, 89, 182, 0.1)",
                ))
                fig_ratio_t.add_hline(y=1.0, line_dash="dash", line_color="gray",
                                      annotation_text="1x")
                fig_ratio_t.update_layout(
                    title="Evolution du ratio median (prix final / mise a prix)",
                    xaxis_title="Mois",
                    yaxis_title="Ratio",
                )
                st.plotly_chart(fig_ratio_t, use_container_width=True)

            # Success rate evolution
            monthly["taux_vente"] = monthly["nb_sold"] / monthly["nb_total"] * 100
            fig_rate = go.Figure()
            fig_rate.add_trace(go.Scatter(
                x=monthly["result_month"],
                y=monthly["taux_vente"],
                mode="lines+markers",
                name="Taux de vente (%)",
                line=dict(color="#2ecc71", width=2),
                fill="tozeroy",
                fillcolor="rgba(46, 204, 113, 0.1)",
            ))
            fig_rate.update_layout(
                title="Taux de vente par mois",
                xaxis_title="Mois",
                yaxis_title="% vendus",
                yaxis=dict(range=[0, 105]),
            )
            st.plotly_chart(fig_rate, use_container_width=True)

    # ================================================================
    # TAB 5 : Geographic analysis
    # ================================================================
    with tab_geo:
        st.subheader("Analyse geographique")

        df_dept = df[df["department_code"].notna()].copy()

        if df_dept.empty:
            st.info("Pas de donnees avec departement.")
        else:
            # Stats by department (detailed)
            dept_stats = (
                df_dept
                .groupby("department_code")
                .agg(
                    total=("licitor_id", "count"),
                    vendus=("result_status", lambda s: (s == "sold").sum()),
                    carences=("result_status", lambda s: (s == "carence").sum()),
                    avg_map=("mise_a_prix", "mean"),
                    avg_final=("final_price", lambda s: s.dropna().mean() if s.notna().any() else None),
                    median_ratio=("ratio", lambda s: s.dropna().median() if s.notna().any() else None),
                )
                .reset_index()
            )
            dept_stats["taux_vente"] = (dept_stats["vendus"] / dept_stats["total"] * 100).round(0)
            dept_stats = dept_stats.sort_values("total", ascending=False)

            # Format
            dept_display = dept_stats.copy()
            dept_display["avg_map"] = dept_display["avg_map"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else ""
            )
            dept_display["avg_final"] = dept_display["avg_final"].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else ""
            )
            dept_display["median_ratio"] = dept_display["median_ratio"].apply(
                lambda x: f"{x:.2f}x" if pd.notna(x) else ""
            )
            dept_display["taux_vente"] = dept_display["taux_vente"].apply(
                lambda x: f"{x:.0f}%" if pd.notna(x) else ""
            )

            dept_display = dept_display.rename(columns={
                "department_code": "Dept",
                "total": "Total",
                "vendus": "Vendus",
                "carences": "Carences",
                "avg_map": "MAP moy.",
                "avg_final": "Prix final moy.",
                "median_ratio": "Ratio median",
                "taux_vente": "Taux vente",
            })

            st.dataframe(dept_display, use_container_width=True, hide_index=True)

            # Scatter : taux de vente vs ratio median per department
            dept_scatter = dept_stats[
                dept_stats["median_ratio"].notna() & (dept_stats["total"] >= 3)
            ].copy()
            if not dept_scatter.empty:
                fig_dep_s = px.scatter(
                    dept_scatter,
                    x="taux_vente",
                    y="median_ratio",
                    size="total",
                    text="department_code",
                    title="Taux de vente vs Ratio median par departement",
                    labels={
                        "taux_vente": "Taux de vente (%)",
                        "median_ratio": "Ratio median",
                        "total": "Nb encheres",
                    },
                    color="avg_final",
                    color_continuous_scale="Viridis",
                )
                fig_dep_s.update_traces(textposition="top center")
                fig_dep_s.add_hline(y=1.0, line_dash="dash", line_color="gray")
                fig_dep_s.update_layout(height=500)
                st.plotly_chart(fig_dep_s, use_container_width=True)

            # By tribunal
            if df["tribunal_name"].notna().any():
                st.subheader("Resultats par tribunal")
                trib_stats = (
                    df[df["tribunal_name"].notna()]
                    .groupby("tribunal_name")
                    .agg(
                        total=("licitor_id", "count"),
                        vendus=("result_status", lambda s: (s == "sold").sum()),
                        avg_final=("final_price", lambda s: s.dropna().mean() if s.notna().any() else None),
                        median_ratio=("ratio", lambda s: s.dropna().median() if s.notna().any() else None),
                    )
                    .reset_index()
                    .sort_values("total", ascending=False)
                )
                trib_stats["taux_vente"] = (trib_stats["vendus"] / trib_stats["total"] * 100).round(0)

                fig_trib = px.bar(
                    trib_stats.head(20),
                    x="tribunal_name",
                    y=["vendus", "total"],
                    title="Volume par tribunal (top 20)",
                    labels={"tribunal_name": "Tribunal", "value": "Nombre"},
                    barmode="group",
                )
                fig_trib.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_trib, use_container_width=True)

                # Table
                trib_display = trib_stats.copy()
                trib_display["avg_final"] = trib_display["avg_final"].apply(
                    lambda x: f"{x:,.0f}" if pd.notna(x) else ""
                )
                trib_display["median_ratio"] = trib_display["median_ratio"].apply(
                    lambda x: f"{x:.2f}x" if pd.notna(x) else ""
                )
                trib_display["taux_vente"] = trib_display["taux_vente"].apply(
                    lambda x: f"{x:.0f}%" if pd.notna(x) else ""
                )
                trib_display = trib_display.rename(columns={
                    "tribunal_name": "Tribunal",
                    "total": "Total",
                    "vendus": "Vendus",
                    "avg_final": "Prix final moy.",
                    "median_ratio": "Ratio median",
                    "taux_vente": "Taux vente",
                })
                st.dataframe(trib_display, use_container_width=True, hide_index=True)
