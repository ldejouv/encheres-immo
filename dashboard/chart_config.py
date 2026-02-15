"""Shared Plotly theme and color constants for the Encheres Immo dashboard."""

PLOTLY_LAYOUT = dict(
    font=dict(family="Inter, sans-serif", size=13, color="#475569"),
    plot_bgcolor="#ffffff",
    paper_bgcolor="#ffffff",
    margin=dict(l=40, r=20, t=50, b=40),
    title_font=dict(size=15, color="#1e293b"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    colorway=[
        "#6366f1",  # indigo
        "#10b981",  # emerald
        "#ef4444",  # red
        "#f59e0b",  # amber
        "#06b6d4",  # cyan
        "#8b5cf6",  # violet
        "#ec4899",  # pink
        "#14b8a6",  # teal
    ],
)

STATUS_LABELS = {
    "sold": "Vendu",
    "carence": "Carence d'encheres",
    "non_requise": "Vente non requise",
}

STATUS_COLORS = {
    "Vendu": "#10b981",
    "Carence d'encheres": "#ef4444",
    "Vente non requise": "#94a3b8",
}

MAP_MARKER_COLORS = {
    "upcoming": "blue",
    "past": "gray",
    "sold": "green",
    "carence": "red",
}


def apply_theme(fig):
    """Apply the standard Encheres Immo theme to a Plotly figure."""
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig
