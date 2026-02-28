# ui/app.py
from __future__ import annotations

import pandas as pd
import streamlit as st

from queries import (
    connect,
    fetch_playoff_field_flexible,
    get_available_seasons,
    get_formula_versions,
    get_rulesets,
)

st.set_page_config(page_title="Fixing College Football — Playoff Viewer", layout="wide")

st.title("🏈 Fixing College Football — Playoff Field Viewer")
st.caption("Read-only UI for inspecting the 24-team playoff field by season (from SQLite).")

with st.sidebar:
    st.header("Settings")

    db_path = st.text_input("SQLite DB path", value="db/league.db")

    try:
        conn = connect(db_path)
    except Exception as e:
        st.error(f"Could not open database: {e}")
        st.stop()

    rulesets = get_rulesets(conn)
    formula_versions = get_formula_versions(conn)

    if not rulesets:
        st.error("No rulesets found in playoff_field_by_year.")
        st.stop()
    if not formula_versions:
        st.error("No formula versions found in playoff_field_by_year.")
        st.stop()

    default_ruleset_idx = rulesets.index("year2") if "year2" in rulesets else 0
    ruleset = st.selectbox("Ruleset", options=rulesets, index=default_ruleset_idx)

    formula_version = st.selectbox("Formula version", options=formula_versions, index=0)

    seasons = get_available_seasons(conn, ruleset, formula_version)
    if not seasons:
        st.error("No seasons found for the selected ruleset + formula version.")
        st.stop()

    season_year = st.selectbox("Season", options=seasons, index=0)

    show_raw = st.toggle("Show raw columns", value=False)

# --- Data load
rows = fetch_playoff_field_flexible(conn, season_year, ruleset, formula_version)
if not rows:
    st.warning("No playoff field rows returned.")
    st.stop()

df = pd.DataFrame([dict(r) for r in rows])

# --- Top summary
team_count = len(df)
bye_count = int(df["is_bye"].sum()) if "is_bye" in df.columns else 0
champ_count = int(df["is_champion"].sum()) if "is_champion" in df.columns else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Season", str(season_year))
c2.metric("Teams in Field", str(team_count))
c3.metric("Bye Teams", str(bye_count))
c4.metric("Auto-bids (Champs)", str(champ_count))

# --- Pot distribution
if "pot" in df.columns:
    pot_counts = df.groupby("pot").size().reset_index(name="teams")
    st.subheader("Pot distribution")
    st.dataframe(pot_counts, use_container_width=True, hide_index=True)

# --- Playoff table
st.subheader("Playoff field")

base_cols = [
    "team_name",
    "conference",
    "pot",
    "bid_type",
    "is_champion",
    "is_bye",
    "conf_coe_rank",
    "conf_rank",
]
rolling_cols = ["total_points_5yr", "points_per_game_5yr"]

cols = [c for c in base_cols if c in df.columns]
for c in rolling_cols:
    if c in df.columns and df[c].notna().any():
        cols.append(c)

display_df = df[cols].copy()

# Make booleans nicer
for bcol in ["is_champion", "is_bye"]:
    if bcol in display_df.columns:
        display_df[bcol] = display_df[bcol].map(lambda x: "✅" if int(x) == 1 else "")

# Optional: raw view
if show_raw:
    st.write("Raw dataset")
    st.dataframe(df, use_container_width=True)

# Highlight bye rows (bye teams are pot=0 -> is_bye = ✅)
def highlight_byes(row):
    if "is_bye" in row.index and row["is_bye"] == "✅":
        return ["font-weight: 700;"] * len(row)
    return [""] * len(row)

st.dataframe(
    display_df.style.apply(highlight_byes, axis=1),
    use_container_width=True,
    hide_index=True,
)

st.caption("Tip: Bye teams are bolded. Champs are marked ✅ in the Champs column.")