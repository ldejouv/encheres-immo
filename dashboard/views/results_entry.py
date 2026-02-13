"""Manual entry page for adjudication results (final prices)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import pandas as pd

from analysis.metrics import AuctionMetrics
from db.database import Database


def render():
    st.header("Saisie des resultats d'adjudication")
    st.markdown(
        "Licitor ne publie pas les prix finaux. Saisissez manuellement les resultats "
        "pour alimenter les analyses de ratios prix final / mise a prix."
    )

    db = Database()
    metrics = AuctionMetrics(db)

    # Search for a listing
    st.subheader("Rechercher un bien")

    col1, col2 = st.columns(2)
    with col1:
        search_id = st.number_input("Numero d'annonce", min_value=0, step=1)
    with col2:
        search_city = st.text_input("Ville (partiel)")

    listing = None
    if search_id > 0:
        listing = db.get_listing_by_licitor_id(search_id)
        if not listing:
            st.warning(f"Annonce n.{search_id} non trouvee dans la base.")
    elif search_city:
        with db.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM listings WHERE LOWER(city) LIKE ? ORDER BY auction_date DESC LIMIT 20",
                (f"%{search_city.lower()}%",),
            ).fetchall()
            if rows:
                results = [dict(r) for r in rows]
                selected = st.selectbox(
                    "Selectionner un bien",
                    range(len(results)),
                    format_func=lambda i: (
                        f"N.{results[i]['licitor_id']} - {results[i]['city']} - "
                        f"{results[i].get('property_type', '')} - "
                        f"{results[i].get('mise_a_prix', 'N/A')} EUR"
                    ),
                )
                listing = results[selected]
            else:
                st.warning("Aucun bien trouve.")

    if listing:
        st.divider()
        st.subheader(f"Annonce n.{listing['licitor_id']}")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Type:** {listing.get('property_type', 'N/A')}")
            st.markdown(f"**Ville:** {listing.get('city', 'N/A')} ({listing.get('department_code', '')})")
            st.markdown(f"**Mise a prix:** {listing.get('mise_a_prix', 'N/A'):,} EUR" if listing.get('mise_a_prix') else "**Mise a prix:** N/A")
        with col2:
            st.markdown(f"**Date:** {listing.get('auction_date', 'N/A')}")
            st.markdown(f"**Surface:** {listing.get('surface_m2', 'N/A')}")
            if listing.get("url_path"):
                st.markdown(f"[Voir sur Licitor](https://www.licitor.com{listing['url_path']})")

        st.divider()

        with st.form("adjudication_form"):
            final_price = st.number_input(
                "Prix final d'adjudication (EUR)",
                min_value=0,
                step=1_000,
            )
            price_source = st.selectbox("Source", ["manual", "external", "estimated"])
            notes = st.text_area("Notes", placeholder="Ex: Adjuge a un investisseur")

            submitted = st.form_submit_button("Enregistrer le resultat")

            if submitted and final_price > 0:
                db.insert_adjudication_result(
                    listing_id=listing["id"],
                    final_price=final_price,
                    price_source=price_source,
                    notes=notes,
                )
                ratio = final_price / listing["mise_a_prix"] if listing.get("mise_a_prix") else 0
                st.success(
                    f"Resultat enregistre: {final_price:,} EUR "
                    f"(ratio: {ratio:.2f}x)" if ratio else f"Resultat enregistre: {final_price:,} EUR"
                )

    # Show existing results
    st.divider()
    st.subheader("Resultats deja saisis")
    df_adj = metrics.adjudication_ratio_analysis()
    if df_adj.empty:
        st.info("Aucun resultat saisi.")
    else:
        st.dataframe(df_adj, width="stretch", hide_index=True)
