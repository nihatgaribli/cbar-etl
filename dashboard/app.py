"""Streamlit dashboard on top of the analytical views.

Run locally:  streamlit run dashboard/app.py
In Docker:    docker compose up -d dashboard   ->  http://localhost:8501
"""
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import altair as alt
import pandas as pd
import psycopg
import streamlit as st
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import Settings  # noqa: E402

st.set_page_config(page_title="AZN exchange rates", page_icon="📈", layout="wide")

# --- palette (validated categorical palette; colors follow the entity, never its rank) ------

LIGHT = {
    "series": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100",
               "#e87ba4", "#008300", "#6250d6", "#e34948"],
    "ink": "#0b0b0b", "ink_secondary": "#52514e", "muted": "#898781",
    "grid": "#e1e0d9", "axis": "#c3c2b7", "surface": "#fcfcfb",
}
DARK = {
    "series": ["#3987e5", "#d95926", "#199e70", "#c98500",
               "#d55181", "#008300", "#9085e9", "#e66767"],
    "ink": "#f0efec", "ink_secondary": "#c3c2b7", "muted": "#898781",
    "grid": "#2c2c2a", "axis": "#383835", "surface": "#1a1a19",
}
THEME = DARK if st.context.theme.type == "dark" else LIGHT

# Fixed entity -> slot mapping, so a currency keeps its color whatever else is selected.
# The default selection takes slots 1-4, which validate as adjacent pairs.
COMPARABLE = ["USD", "EUR", "XAU", "RUB", "GBP", "TRY", "CNY", "GEL"]
DEFAULT_SELECTION = COMPARABLE[:4]
COLORS = dict(zip(COMPARABLE, THEME["series"], strict=True))
PERIODS = {"1M": 30, "3M": 91, "1Y": 365, "5Y": 5 * 365, "All": None}


def date_axis(frame: pd.DataFrame) -> alt.X:
    """One tick format for the span actually shown, instead of Vega's mixed multi-level labels."""
    span = (frame["rate_date"].max() - frame["rate_date"].min()).days if len(frame) else 0
    if span <= 92:
        fmt, ticks = "%d %b", {"interval": "week", "step": 1}
    elif span <= 2 * 365:
        fmt, ticks = "%b %Y", {"interval": "month", "step": 1 if span <= 400 else 3}
    else:
        fmt, ticks = "%Y", {"interval": "year", "step": 1 if span <= 12 * 365 else 2}
    return alt.X("rate_date:T", title=None,
                 axis=alt.Axis(format=fmt, tickCount=ticks, labelOverlap=True))


def dodge(values: pd.Series, min_gap: float) -> pd.Series:
    """Spread label positions so that no two are closer than min_gap; order is preserved."""
    ordered = values.sort_values()
    positions: list[float] = []
    for value in ordered:
        positions.append(value if not positions else max(value, positions[-1] + min_gap))
    return pd.Series(positions, index=ordered.index).reindex(values.index)


def style(chart: alt.Chart, height: int = 320) -> alt.Chart:
    """Recessive chrome: hairline solid grid, muted axes, no chart border."""
    return (
        chart.properties(height=height)
        .configure_view(strokeWidth=0)
        .configure_axis(gridColor=THEME["grid"], domainColor=THEME["axis"],
                        tickColor=THEME["axis"], labelColor=THEME["muted"],
                        titleColor=THEME["ink_secondary"], labelFontSize=12)
        .configure_legend(labelColor=THEME["ink_secondary"], orient="top", title=None,
                          symbolType="stroke", symbolStrokeWidth=2)
    )


# --- data access -----------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg.connect(**Settings.from_env().conninfo(), row_factory=dict_row) as conn:
        rows = conn.execute(sql, params).fetchall()
    frame = pd.DataFrame(rows)
    for column in frame.columns:
        if len(frame) and isinstance(frame[column].iloc[0], Decimal):
            frame[column] = frame[column].astype(float)
    return frame


def history(codes: list[str], start: date) -> pd.DataFrame:
    frame = query(
        "SELECT currency_code AS code, rate_date, rate_per_unit FROM exchange_rates "
        "WHERE currency_code = ANY(%s) AND rate_date >= %s ORDER BY rate_date",
        (codes, start),
    )
    if not frame.empty:
        frame["rate_date"] = pd.to_datetime(frame["rate_date"])
        first = frame.groupby("code")["rate_per_unit"].transform("first")
        frame["indexed"] = frame["rate_per_unit"] / first * 100
    return frame


try:
    latest = query("SELECT * FROM v_latest_rates ORDER BY code")
except psycopg.OperationalError as exc:
    st.error(f"Cannot reach the database: {exc}")
    st.stop()

if latest.empty:
    st.info("No data yet. Run the pipeline first: `docker compose run --rm etl`")
    st.stop()

coverage = query("SELECT MIN(rate_date) AS first, MAX(rate_date) AS last, "
                 "COUNT(DISTINCT rate_date) AS bulletins, COUNT(*) AS rows FROM exchange_rates")
last_date = coverage.at[0, "last"]

st.title("AZN exchange rates")
st.caption(
    "Official rates of the Central Bank of Azerbaijan · "
    f"latest bulletin **{last_date:%d.%m.%Y}** · "
    f"{coverage.at[0, 'bulletins']:,} bulletins and {coverage.at[0, 'rows']:,} rates since "
    f"{coverage.at[0, 'first']:%d.%m.%Y}"
)

# --- headline tiles --------------------------------------------------------------------------

TILES = [("USD", 1, "US dollar"), ("EUR", 1, "Euro"), ("RUB", 100, "100 Russian rubles"),
         ("XAU", 1, "Gold, troy ounce")]
spark = history([code for code, _, _ in TILES], last_date - timedelta(days=45))
for column, (code, multiplier, label) in zip(st.columns(len(TILES)), TILES, strict=True):
    row = latest.loc[latest["code"] == code].iloc[0]
    column.metric(
        label=f"{label} · {code}",
        value=f"{row['rate_per_unit'] * multiplier:,.4f} ₼",
        delta=None if pd.isna(row["change_pct"]) else f"{row['change_pct']:+.2f}%",
        delta_color="off",  # a move is not "good" or "bad" here, so no status color
        delta_description="vs previous bulletin",
        chart_data=spark.loc[spark["code"] == code, "rate_per_unit"].tolist(),
        border=True,
    )

# --- filters: one row, scoping the comparison section ----------------------------------------

st.subheader("Compare currencies")
left, middle, right = st.columns([3, 1.3, 1.6])
codes = left.multiselect("Currencies", COMPARABLE, default=DEFAULT_SELECTION,
                         format_func=lambda c: f"{c} · {latest.set_index('code').at[c, 'name_az']}")
period = middle.segmented_control("Period", list(PERIODS), default="1Y", required=True)
mode = right.segmented_control("Scale", ["Indexed", "AZN per unit"], default="Indexed",
                               required=True,
                               help="Indexed puts every currency on one axis (start = 100). "
                                    "AZN per unit shows real prices, one chart per currency.")

days = PERIODS[period]
start = date(2006, 1, 1) if days is None else last_date - timedelta(days=days)
data = history(codes, start) if codes else pd.DataFrame()

if data.empty:
    st.info("Select at least one currency.")
elif mode == "Indexed":
    color = alt.Color("code:N", scale=alt.Scale(domain=codes, range=[COLORS[c] for c in codes]),
                      legend=alt.Legend(title=None))
    base = alt.Chart(data).encode(x=date_axis(data))
    lines = base.mark_line(strokeWidth=2).encode(
        y=alt.Y("indexed:Q", title=f"Index, {start:%d.%m.%Y} = 100", scale=alt.Scale(zero=False)),
        color=color,
    )
    hover = alt.selection_point(nearest=True, on="pointerover", fields=["rate_date"], empty=False)
    crosshair = (
        base.transform_pivot("code", value="indexed", groupby=["rate_date"])
        .mark_rule(color=THEME["muted"], strokeWidth=1)
        .encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[alt.Tooltip("rate_date:T", title="Date", format="%d.%m.%Y")]
            + [alt.Tooltip(f"{c}:Q", format=".1f") for c in codes],
        )
        .add_params(hover)
    )
    points = lines.mark_point(size=64, filled=True, stroke=THEME["surface"], strokeWidth=2).encode(
        opacity=alt.condition(hover, alt.value(1), alt.value(0))
    )
    # Direct labels at the line ends, in ink (the colored end dot carries identity).
    # Dots sit on the true value; labels are spread apart so close lines stay readable.
    ends = data.sort_values("rate_date").groupby("code").tail(1).copy()
    y_span = data["indexed"].max() - data["indexed"].min()
    ends["label_y"] = dodge(ends["indexed"], max(y_span, 1) * 0.06)
    ends["label"] = ends["code"] + " " + ends["indexed"].round().astype(int).astype(str)
    end_base = alt.Chart(ends).encode(x="rate_date:T")
    end_dots = end_base.mark_point(size=64, filled=True, opacity=1).encode(
        y="indexed:Q", color=color)
    end_labels = end_base.mark_text(
        align="left", dx=8, color=THEME["ink_secondary"], fontSize=12
    ).encode(y="label_y:Q", text="label:N")
    chart = alt.layer(lines, crosshair, points, end_dots, end_labels).properties(
        padding={"right": 70}
    )
    st.altair_chart(style(chart, 380), width="stretch")
else:
    columns = st.columns(2)
    for index, code in enumerate(codes):
        subset = data.loc[data["code"] == code]
        chart = alt.Chart(subset, title=alt.Title(
            f"{code} · AZN per {'troy ounce' if code == 'XAU' else 'unit'}",
            color=THEME["ink"], fontSize=14, anchor="start")
        ).mark_line(strokeWidth=2, color=COLORS[code]).encode(
            x=date_axis(subset),
            y=alt.Y("rate_per_unit:Q", title=None, scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("rate_date:T", title="Date", format="%d.%m.%Y"),
                     alt.Tooltip("rate_per_unit:Q", title="AZN", format=",.4f")],
        )
        columns[index % 2].altair_chart(style(chart, 240), width="stretch")

if not data.empty:
    with st.expander("Show data"):
        table = data.pivot(index="rate_date", columns="code", values="rate_per_unit")
        st.dataframe(table.sort_index(ascending=False), width="stretch")

# --- USD history with devaluations -----------------------------------------------------------

st.subheader("US dollar since 2006 and the 2015 devaluations")
usd = history(["USD"], date(2006, 1, 1))
shocks = query("SELECT rate_date, prev_rate, rate_per_unit, change_pct FROM v_shocks "
               "WHERE currency_code = 'USD' ORDER BY rate_date")
usd_line = alt.Chart(usd).mark_line(strokeWidth=2, color=COLORS["USD"]).encode(
    x=date_axis(usd),
    y=alt.Y("rate_per_unit:Q", title="AZN per USD", scale=alt.Scale(zero=False)),
    tooltip=[alt.Tooltip("rate_date:T", title="Date", format="%d.%m.%Y"),
             alt.Tooltip("rate_per_unit:Q", title="AZN", format=".4f")],
)
layers = [usd_line]
if not shocks.empty:
    shocks["rate_date"] = pd.to_datetime(shocks["rate_date"])
    shocks["label"] = shocks.apply(
        lambda r: f"{r['rate_date']:%d.%m.%Y}  {r['change_pct']:+.1f}%", axis=1)
    marks = alt.Chart(shocks).encode(x="rate_date:T", y="rate_per_unit:Q")
    layers += [
        marks.mark_point(size=90, filled=True, color=COLORS["USD"],
                         stroke=THEME["surface"], strokeWidth=2).encode(
            tooltip=[alt.Tooltip("rate_date:T", title="Date", format="%d.%m.%Y"),
                     alt.Tooltip("prev_rate:Q", title="Before", format=".4f"),
                     alt.Tooltip("rate_per_unit:Q", title="After", format=".4f"),
                     alt.Tooltip("change_pct:Q", title="Change %", format="+.2f")]),
        # Label only the devaluations; smaller shocks keep their dot and tooltip
        marks.transform_filter("abs(datum.change_pct) >= 10")
        .mark_text(align="right", dx=-10, dy=-4, color=THEME["ink_secondary"], fontSize=12)
        .encode(text="label:N"),
    ]
st.altair_chart(style(alt.layer(*layers), 300), width="stretch")
if not shocks.empty:
    with st.expander("Show data"):
        st.dataframe(shocks.drop(columns="label"), hide_index=True, width="stretch")

# --- volatility ranking -----------------------------------------------------------------------

st.subheader("Most volatile currencies, last 365 days")
volatility = query(
    """SELECT d.currency_code AS code, c.name_az,
              ROUND(STDDEV_SAMP(d.change_pct), 3) AS volatility_pct
       FROM v_daily_change AS d JOIN currencies AS c ON c.code = d.currency_code
       WHERE c.category = 'currency' AND d.rate_date > %s
       GROUP BY d.currency_code, c.name_az
       ORDER BY volatility_pct DESC NULLS LAST LIMIT 10""",
    (last_date - timedelta(days=365),),
)
bars = alt.Chart(volatility).encode(
    y=alt.Y("code:N", sort="-x", title=None),
    x=alt.X("volatility_pct:Q", title="Std. deviation of daily change, %",
            axis=alt.Axis(tickCount=6)),
    tooltip=[alt.Tooltip("name_az:N", title="Currency"),
             alt.Tooltip("volatility_pct:Q", title="Volatility %", format=".3f")],
)
volatility_chart = alt.layer(
    bars.mark_bar(color=COLORS["USD"], cornerRadiusEnd=4, size=16),
    bars.mark_text(align="left", dx=6, color=THEME["ink_secondary"], fontSize=12)
    .encode(text=alt.Text("volatility_pct:Q", format=".3f")),
)
st.altair_chart(style(volatility_chart, 340), width="stretch")
st.caption("Volatility = standard deviation of day-over-day % changes. USD sits near zero "
           "because the manat has been held at 1.70 per dollar.")

# --- all rates and pipeline health -----------------------------------------------------------

st.subheader(f"All rates on {last_date:%d.%m.%Y}")
st.dataframe(
    latest[["code", "name_az", "category", "unit", "rate_per_unit", "prev_rate", "change_pct"]],
    hide_index=True, width="stretch",
    column_config={
        "rate_per_unit": st.column_config.NumberColumn("AZN per unit", format="%.6f"),
        "prev_rate": st.column_config.NumberColumn("Previous", format="%.6f"),
        "change_pct": st.column_config.NumberColumn("Change %", format="%+.2f"),
    },
)

st.subheader("Pipeline runs")
runs = query("SELECT run_id, status, started_at, duration::text AS duration, date_from, date_to, "
             "bulletins_loaded, rows_inserted, rows_updated, rows_unchanged, rows_skipped "
             "FROM v_etl_runs LIMIT 15")
st.dataframe(runs, hide_index=True, width="stretch")
