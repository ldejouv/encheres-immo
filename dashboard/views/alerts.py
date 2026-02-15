"""Alert management - create, edit, delete alert criteria and view matches."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import pandas as pd

from analysis.metrics import AuctionMetrics
from db.database import Database


def render():
    """Legacy entry point — redirects to admin.py composite page."""
    from dashboard.views.admin import render as render_admin
    render_admin()


def render_alerts_tab():
    """Render the alerts management tab (embedded in admin page)."""
    db = Database()
    metrics = AuctionMetrics(db)

    # Load filter options for multiselect widgets
    opts = metrics.get_historical_filter_options()

    # ── Create new alert ─────────────────────────────────────────────
    st.subheader("Creer une nouvelle alerte")

    with st.form("admin_alerts_new_alert"):
        name = st.text_input("Nom de l'alerte", placeholder="Ex: Appart Paris < 100k", key="admin_alerts_name")

        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Prix minimum (EUR)", min_value=0, value=0, step=10_000, key="admin_alerts_min_price")
            max_price = st.number_input("Prix maximum (EUR)", min_value=0, value=0, step=10_000, key="admin_alerts_max_price")
            min_surface = st.number_input("Surface min (m2)", min_value=0.0, value=0.0, step=5.0, key="admin_alerts_min_surf")
            max_surface = st.number_input("Surface max (m2)", min_value=0.0, value=0.0, step=5.0, key="admin_alerts_max_surf")

        with col2:
            sel_depts = st.multiselect(
                "Departements",
                opts.get("departments", []),
                key="admin_alerts_depts",
            )
            sel_regions = st.multiselect(
                "Regions",
                opts.get("regions", []),
                key="admin_alerts_regions",
            )
            sel_types = st.multiselect(
                "Types de bien",
                opts.get("property_types", []),
                key="admin_alerts_types",
            )

        submitted = st.form_submit_button("Creer l'alerte")

        if submitted and name:
            data = {
                "name": name,
                "min_price": min_price if min_price > 0 else None,
                "max_price": max_price if max_price > 0 else None,
                "department_codes": ",".join(sel_depts) if sel_depts else None,
                "regions": ",".join(sel_regions) if sel_regions else None,
                "property_types": ",".join(sel_types) if sel_types else None,
                "min_surface": min_surface if min_surface > 0 else None,
                "max_surface": max_surface if max_surface > 0 else None,
            }
            alert_id = db.create_alert(data)
            st.success(f"Alerte '{name}' creee (ID: {alert_id})")
            st.rerun()

    st.divider()

    # ── Active alerts ────────────────────────────────────────────────
    st.subheader("Alertes actives")

    alerts = db.get_active_alerts()
    if not alerts:
        st.info("Aucune alerte configuree.")
    else:
        for alert in alerts:
            with st.expander(
                f"{'[ON]' if alert['is_active'] else '[OFF]'} {alert['name']}",
                expanded=False,
            ):
                st.markdown(f"**ID:** {alert['id']}")
                if alert["min_price"] or alert["max_price"]:
                    st.markdown(
                        f"**Prix:** {alert.get('min_price', '-')} - {alert.get('max_price', '-')} EUR"
                    )
                if alert["department_codes"]:
                    st.markdown(f"**Departements:** {alert['department_codes']}")
                if alert["regions"]:
                    st.markdown(f"**Regions:** {alert['regions']}")
                if alert["property_types"]:
                    st.markdown(f"**Types:** {alert['property_types']}")
                if alert["min_surface"] or alert["max_surface"]:
                    st.markdown(
                        f"**Surface:** {alert.get('min_surface', '-')} - {alert.get('max_surface', '-')} m2"
                    )

                col1, col2 = st.columns(2)
                if col1.button(
                    "Activer/Desactiver", key=f"admin_alerts_toggle_{alert['id']}"
                ):
                    db.toggle_alert(alert["id"])
                    st.rerun()
                if col2.button(
                    "Supprimer", key=f"admin_alerts_delete_{alert['id']}"
                ):
                    db.delete_alert(alert["id"])
                    st.rerun()

    st.divider()

    # ── Alert matches ────────────────────────────────────────────────
    st.subheader("Correspondances non lues")

    matches = db.get_unread_matches()
    if not matches:
        st.info("Aucune nouvelle correspondance.")
    else:
        st.markdown(f"**{len(matches)}** nouvelles correspondances")

        df = pd.DataFrame(matches)
        display_cols = [
            "alert_name", "licitor_id", "city", "property_type",
            "mise_a_prix", "auction_date",
        ]
        available = [c for c in display_cols if c in df.columns]
        df_display = df[available].copy()
        if "mise_a_prix" in df_display.columns:
            df_display["mise_a_prix"] = df_display["mise_a_prix"].apply(
                lambda x: f"{x:,.0f} EUR" if pd.notna(x) else ""
            )
        st.dataframe(df_display, use_container_width=True, hide_index=True)

        if st.button("Marquer tout comme lu", key="admin_alerts_mark_read"):
            match_ids = [m["match_id"] for m in matches]
            db.mark_matches_seen(match_ids)
            st.success("Correspondances marquees comme lues.")
            st.rerun()
