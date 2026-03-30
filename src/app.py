# src/app.py

from pathlib import Path
from src.common import log_timed_block, reset_log, log_message, benchmark_call
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
from src.config import ChartConfig, Severity

from src.config import OUTPUT_DIR
from src.analytics import collision_severity_analysis, collisions_by_day_of_week, collisions_by_division, collisions_by_hour, collisions_by_month, collisions_by_neighbourhood, forecast_collision_trend, road_user_analysis, severity_trend_over_time, total_collisions_trend_over_time

# Dashboard with day-of-week and monthly analysis (US-12)


DEFAULT_PREPARED_DATASET_PATH = Path("data/processed/traffic_collisions_prepared.parquet")
LOG_FILE = Path("logs/app_benchmark.log")


@st.cache_data
def get_prepared_data(dataset_path: str):
    end_log = log_timed_block("get_prepared_data")
    df = pd.read_parquet(dataset_path)
    end_log()
    return df


def apply_collision_severity_filter(df: pd.DataFrame, severity: str) -> pd.DataFrame:
    end_log = log_timed_block("apply_collision_severity_filter")

    result = df.copy()

    if severity == "All Severities":
        end_log()
        return result
    if severity == "Fatal":
        result = result[result["severity_class"] == "Fatal"]
        end_log()
        return result
    if severity == "Injury":
        result = result[result["severity_class"] == "Injury"]
        end_log()
        return result
    if severity == "Property Damage":
        result = result[result["severity_class"] == "Property Damage"]
        end_log()
        return result
    if severity == "Other":
        result = result[result["severity_class"] == "Other"]
        end_log()
        return result

    end_log()
    return result


def apply_recent_days_filter(df: pd.DataFrame, recent_days: int | None) -> pd.DataFrame:
    end_log = log_timed_block("apply_recent_days_filter")
    result = df.copy()

    if recent_days is None or "OCC_DATE" not in result.columns:
        end_log()
        return result

    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")
    max_date = result["OCC_DATE"].max()

    if pd.isna(max_date):
        end_log()
        return result

    start_date = max_date - pd.Timedelta(days=recent_days)
    result = result[result["OCC_DATE"] >= start_date].copy()

    end_log()
    return result


def filter_collisions_prepared(
    df: pd.DataFrame,
    years: list[int] | None = None,
    divisions: list[str] | None = None,
    neighbourhoods: list[str] | None = None,
) -> pd.DataFrame:
    end_log = log_timed_block("filter_collisions_prepared")
    result = df.copy()

    if years:
        if "YEAR" in result.columns:
            result = result[result["YEAR"].isin(years)]
        elif "OCC_YEAR" in result.columns:
            result = result[result["OCC_YEAR"].isin(years)]

    if divisions:
        result = result[result["DIVISION"].isin(divisions)]

    if neighbourhoods:
        result = result[result["NEIGHBOURHOOD_158"].isin(neighbourhoods)]

    end_log()
    return result


def apply_road_user_filter(
    df: pd.DataFrame,
    pedestrian: bool = False,
    bicycle: bool = False,
    motorcycle: bool = False,
) -> pd.DataFrame:
    end_log = log_timed_block("apply_road_user_filter")

    result = df.copy()
    selected_conditions = []

    if pedestrian and "PEDESTRIAN" in result.columns:
        selected_conditions.append(result["PEDESTRIAN"].eq("YES"))
    if bicycle and "BICYCLE" in result.columns:
        selected_conditions.append(result["BICYCLE"].eq("YES"))
    if motorcycle and "MOTORCYCLE" in result.columns:
        selected_conditions.append(result["MOTORCYCLE"].eq("YES"))

    if not selected_conditions:
        end_log()
        return result

    combined_condition = selected_conditions[0]
    for cond in selected_conditions[1:]:
        combined_condition = combined_condition | cond

    result = result[combined_condition].copy()

    end_log()
    return result

def build_severity_map_dataframe(filtered_df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("build_severity_map_dataframe")

    required = ["LAT_WGS84", "LONG_WGS84", "severity_class"]
    if not all(col in filtered_df.columns for col in required):
        end_log()
        return pd.DataFrame()

    columns = [
        "LAT_WGS84",
        "LONG_WGS84",
        "NEIGHBOURHOOD_158",
        "OCC_HOUR",
        "DIVISION",
        "FATALITIES",
        "OCC_DATE_STR",
        "severity_class",
        "lat_cluster",
        "lon_cluster",
    ]
    available_columns = [c for c in columns if c in filtered_df.columns]

    map_df = filtered_df[available_columns].dropna(
        subset=["LAT_WGS84", "LONG_WGS84"]
    ).copy()

    end_log()
    return map_df


def build_cluster_dataframe(map_df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("build_cluster_dataframe")

    if map_df.empty or "lat_cluster" not in map_df.columns or "lon_cluster" not in map_df.columns:
        end_log()
        return pd.DataFrame()

    grouped = (
        map_df.groupby(["lat_cluster", "lon_cluster", "severity_class"])
        .size()
        .reset_index(name="collision_count")
        .rename(columns={"lat_cluster": "LAT_WGS84", "lon_cluster": "LONG_WGS84"})
    )

    end_log()
    return grouped


def get_severity_color_map() -> dict[str, list[int]]:
    end_log = log_timed_block("get_severity_color_map")
    result = {
        "Fatal": [220, 53, 69, 210],
        "Injury": [255, 140, 0, 190],
        "Property Damage": [255, 193, 7, 180],
        "Other": [108, 117, 125, 150],
    }
    end_log()
    return result


def render_map_legend():
    end_log = log_timed_block("render_map_legend")
    components.html(
        """
        <div style="padding:5px 5px; border:1px solid #ddd; border-radius:8px; background-color:#fafafa; margin-top:8px;">
            <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
                <div style="display:flex; align-items:center; gap:6px;">
                    <div style="width:14px; height:14px; border-radius:50%; background:#dc3545;"></div>
                    <span>Fatal</span>
                </div>
                <div style="display:flex; align-items:center; gap:6px;">
                    <div style="width:14px; height:14px; border-radius:50%; background:#ff8c00;"></div>
                    <span>Injury</span>
                </div>
                <div style="display:flex; align-items:center; gap:6px;">
                    <div style="width:14px; height:14px; border-radius:50%; background:#ffc107;"></div>
                    <span>Property Damage</span>
                </div>
                <div style="display:flex; align-items:center; gap:6px;">
                    <div style="width:14px; height:14px; border-radius:50%; background:#6c757d;"></div>
                    <span>Other</span>
                </div>
            </div>
        </div>
        """,
        height=95,
    )
    end_log()


def build_map(filtered_df: pd.DataFrame, map_mode: str, map_style_option: str):
    end_build_map = log_timed_block("build_map")
    log_message(f"MAP VIEW selected: {map_mode}")

    map_df = build_severity_map_dataframe(filtered_df)

    if map_df.empty:
        end_build_map()
        return None, "No valid coordinates available for the current filter selection."

    # Preserve all fatal points, sample the rest if needed
    end_sample = log_timed_block("build_map.sample_limit")
    if len(map_df) > 6000:
        fatal_df = map_df[map_df["severity_class"] == "Fatal"]
        non_fatal_df = map_df[map_df["severity_class"] != "Fatal"]

        remaining = max(6000 - len(fatal_df), 0)
        if len(non_fatal_df) > remaining:
            non_fatal_df = non_fatal_df.sample(remaining, random_state=42)

        map_df = pd.concat([fatal_df, non_fatal_df], ignore_index=True)
    end_sample()

    end_center = log_timed_block("build_map.compute_center")
    center_lat = map_df["LAT_WGS84"].mean()
    center_lon = map_df["LONG_WGS84"].mean()

    lat_range = map_df["LAT_WGS84"].max() - map_df["LAT_WGS84"].min()
    lon_range = map_df["LONG_WGS84"].max() - map_df["LONG_WGS84"].min()
    spread = max(lat_range, lon_range)

    if spread < 0.01:
        zoom = 16
    elif spread < 0.05:
        zoom = 15
    elif spread < 0.1:
        zoom = 14
    elif spread < 0.5:
        zoom = 13
    else:
        zoom = 12
    end_center()

    color_map = get_severity_color_map()
    layers = []

    if map_mode == "Severity Point Map":
        end_mode = log_timed_block("build_map.point_layers")
        severity_draw_order = ["Other", "Property Damage", "Injury", "Fatal"]

        for severity in severity_draw_order:
            color = color_map[severity]
            subset = map_df[map_df["severity_class"] == severity].copy()
            if subset.empty:
                continue

            radius = (
                80 if severity == "Fatal"
                else 55 if severity == "Injury"
                else 42 if severity == "Property Damage"
                else 35
            )

            layers.append(
                pdk.Layer(
                    "ScatterplotLayer",
                    data=subset,
                    get_position=["LONG_WGS84", "LAT_WGS84"],
                    get_fill_color=color,
                    get_radius=radius,
                    pickable=True,
                    stroked=True,
                    get_line_color=[255, 255, 255, 120],
                    line_width_min_pixels=1,
                    filled=True,
                )
            )
        end_mode()

    elif map_mode == "Severity Heatmap":
        end_mode = log_timed_block("build_map.heatmap_layers")
        for severity in ["Fatal", "Injury", "Property Damage"]:
            subset = map_df[map_df["severity_class"] == severity].copy()
            if subset.empty:
                continue

            layers.append(
                pdk.Layer(
                    "HeatmapLayer",
                    data=subset,
                    get_position=["LONG_WGS84", "LAT_WGS84"],
                    pickable=True,
                    opacity=0.65,
                )
            )
        end_mode()

    elif map_mode == "Severity Cluster Bubbles":
        cluster_df = build_cluster_dataframe(map_df)

        if cluster_df.empty:
            end_build_map()
            return None, "No cluster data available for the current filter selection."

        end_mode = log_timed_block("build_map.cluster_layers")
        severity_draw_order = ["Other", "Property Damage", "Injury", "Fatal"]

        for severity in severity_draw_order:
            color = color_map[severity]
            subset = cluster_df[cluster_df["severity_class"] == severity].copy()
            if subset.empty:
                continue

            layers.append(
                pdk.Layer(
                    "ScatterplotLayer",
                    data=subset,
                    get_position=["LONG_WGS84", "LAT_WGS84"],
                    get_fill_color=color,
                    get_radius="collision_count * 20",
                    radius_min_pixels=8,
                    radius_max_pixels=50,
                    pickable=True,
                    stroked=True,
                    get_line_color=[255, 255, 255, 140],
                    line_width_min_pixels=1,
                    filled=True,
                )
            )

            text_df = subset.copy()
            text_df["label"] = text_df["collision_count"].astype(str)

            layers.append(
                pdk.Layer(
                    "TextLayer",
                    data=text_df,
                    get_position=["LONG_WGS84", "LAT_WGS84"],
                    get_text="label",
                    get_size=12,
                    get_color=[0, 0, 0, 220],
                    get_alignment_baseline="'center'",
                )
            )
        end_mode()

    end_tooltip = log_timed_block("build_map.tooltip")
    tooltip = {
        "html": """
            <b>Severity:</b> {severity_class} <br/>
            <b>Neighbourhood:</b> {NEIGHBOURHOOD_158} <br/>
            <b>Date:</b> {OCC_DATE_STR} <br/>
            <b>Hour:</b> {OCC_HOUR} <br/>
            <b>Division:</b> {DIVISION} <br/>
            <b>Fatalities:</b> {FATALITIES}
        """,
        "style": {
            "fontSize": "12px",
            "backgroundColor": "white",
            "color": "black",
        },
    }

    if map_mode == "Severity Cluster Bubbles":
        tooltip = {
            "html": """
                <b>Severity:</b> {severity_class} <br/>
                <b>Cluster Count:</b> {collision_count}
            """,
            "style": {
                "fontSize": "12px",
                "backgroundColor": "white",
                "color": "black",
            },
        }
    end_tooltip()

    end_deck = log_timed_block("build_map.deck")
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=zoom,
            pitch=0,
        ),
        tooltip=tooltip,
        map_style=map_style_option,
    )
    end_deck()

    end_build_map()
    return deck, None


def export_results(df: pd.DataFrame, output_path: str) -> str:
    end_log = log_timed_block("export_results")
    df.to_csv(output_path, index=False)
    end_log()
    return output_path


def main() -> None:
    reset_log()
    timings = []

    total_end = log_timed_block("main_total_runtime")

    st.set_page_config(page_title="Toronto Collision Risk Dashboard", layout="wide")

    st.title("Toronto Collision Risk Dashboard")


    dataset_path = st.sidebar.text_input(
        "Prepared dataset path",
        str(DEFAULT_PREPARED_DATASET_PATH),
    )

    try:
        df = benchmark_call(timings, "get_prepared_data", get_prepared_data, dataset_path)
    except Exception as exc:
        st.error(f"Failed to load prepared dataset: {exc}")
        return

    st.sidebar.header("Filters")

    # 1. Time Window
    recent_days_option = st.sidebar.selectbox(
        "Time Window",
        [
            "All Time",
            "7 Days",
            "14 Days",
            "30 Days",
            "60 Days",
            "90 Days",
            "180 Days",
            "365 Days",
        ],
        index=3,  # still default to 90 Days
    )
    log_message(f"TREND WINDOW selected: {recent_days_option}")
    show_forecast = st.sidebar.checkbox("Show Forecast", value=True)
    forecast_horizon_option = st.sidebar.selectbox(
        "Forecast Horizon",
        [
            "7 Days",
            "14 Days",
            "30 Days",
            "60 Days",
            "90 Days",
        ],
        index=0,  # default still 7 Days
    )

    forecast_horizon_lookup = {
        "7 Days": 7,
        "14 Days": 14,
        "30 Days": 30,
        "60 Days": 60,
        "90 Days": 90,
    }

    forecast_horizon = forecast_horizon_lookup[forecast_horizon_option]
    log_message(f"SHOW FORECAST selected: {show_forecast}")
    log_message(f"FORECAST HORIZON selected: {forecast_horizon}")

    recent_days_lookup = {
        "All Time": None,
        "7 Days": 7,
        "14 Days": 14,
        "30 Days": 30,
        "60 Days": 60,
        "90 Days": 90,
        "180 Days": 180,
        "365 Days": 365,
    }
    recent_days = recent_days_lookup[recent_days_option]

    # 2. Year
    end_years = log_timed_block("sidebar.years")
    years = (
        sorted([int(y) for y in df["YEAR"].dropna().unique().tolist()])
        if "YEAR" in df.columns
        else []
    )
    default_year = [max(years)] if years else []
    selected_years = st.sidebar.multiselect("Year", years, default=default_year)
    end_years()

    # 3. Division
    end_divisions = log_timed_block("sidebar.divisions")
    divisions = (
        sorted(df["DIVISION"].dropna().astype(str).unique().tolist())
        if "DIVISION" in df.columns
        else []
    )
    selected_divisions = st.sidebar.multiselect("Division", divisions)
    end_divisions()

    # 4. Neighbourhood
    end_neigh = log_timed_block("sidebar.neighbourhoods")
    neighbourhoods = (
        sorted(df["NEIGHBOURHOOD_158"].dropna().astype(str).unique().tolist())
        if "NEIGHBOURHOOD_158" in df.columns
        else []
    )
    selected_neighbourhoods = st.sidebar.multiselect("Neighbourhood", neighbourhoods)
    end_neigh()

    # 5. Severity
    collision_severity = st.sidebar.selectbox(
        "Collision Severity",
        ["All Severities", "Fatal", "Injury", "Property Damage", "Other"],
    )
    log_message(f"COLLISION SEVERITY selected: {collision_severity}")

    # Top N control
    top_n_option = st.sidebar.selectbox("Top N", [5, 10, 15], index=1)
    log_message(f"TOP N selected: {top_n_option}")

    # 6. Road User
    st.sidebar.markdown("**Road User Involvement**")
    filter_pedestrian = st.sidebar.checkbox("Pedestrian")
    filter_bicycle = st.sidebar.checkbox("Bicycle")
    filter_motorcycle = st.sidebar.checkbox("Motorcycle")

    selected_road_users = {
        "Pedestrian": filter_pedestrian,
        "Bicycle": filter_bicycle,
        "Motorcycle": filter_motorcycle,
    }
    log_message(f"ROAD USER FILTERS selected: {selected_road_users}")

    # 7. Map View
    map_mode = st.sidebar.radio(
        "Map View",
        ["Severity Point Map", "Severity Heatmap", "Severity Cluster Bubbles"],
    )
    log_message(f"MAP MODE selected in sidebar: {map_mode}")

    # 8. Map Style
    map_style_display = st.sidebar.selectbox(
        "Map Style",
        ["Light", "Road", "Dark"],
        index=0,
    )
    log_message(f"MAP STYLE selected: {map_style_display}")

    map_style_lookup = {
        "Light": "light",
        "Road": "road",
        "Dark": "dark",
    }
    map_style_option = map_style_lookup[map_style_display]

    filtered_df = benchmark_call(
        timings,
        "filter_collisions_prepared",
        filter_collisions_prepared,
        df,
        years=selected_years or None,
        divisions=selected_divisions or None,
        neighbourhoods=selected_neighbourhoods or None,
    )
    filtered_df = benchmark_call(
        timings,
        "apply_collision_severity_filter",
        apply_collision_severity_filter,
        filtered_df,
        collision_severity,
    )
    filtered_df = benchmark_call(
        timings,
        "apply_road_user_filter",
        apply_road_user_filter,
        filtered_df,
        pedestrian=filter_pedestrian,
        bicycle=filter_bicycle,
        motorcycle=filter_motorcycle,
    )
    filtered_df = benchmark_call(
        timings,
        "apply_recent_days_filter",
        apply_recent_days_filter,
        filtered_df,
        recent_days,
    )

    hourly_df = (
        benchmark_call(timings, "collisions_by_hour", collisions_by_hour, filtered_df)
        if not filtered_df.empty
        else pd.DataFrame()
    )
    neighbourhood_df = (
        benchmark_call(
            timings,
            "collisions_by_neighbourhood",
            collisions_by_neighbourhood,
            filtered_df,
            top_n=top_n_option,
        )
        if not filtered_df.empty
        else pd.DataFrame()
    )
    division_df = (
        benchmark_call(
            timings,
            "collisions_by_division",
            collisions_by_division,
            filtered_df,
            top_n=top_n_option,
        )
        if not filtered_df.empty
        else pd.DataFrame()
    )
    severity_df = (
        benchmark_call(
            timings,
            "collision_severity_analysis",
            collision_severity_analysis,
            filtered_df,
        )
        if not filtered_df.empty
        else pd.DataFrame()
    )
    road_users_df = (
        benchmark_call(
            timings,
            "road_user_analysis",
            road_user_analysis,
            filtered_df,
        )
        if not filtered_df.empty
        else pd.DataFrame()
    )
    trend_df = benchmark_call(
        timings,
        "severity_trend_over_time",
        severity_trend_over_time,
        filtered_df,
        collision_severity,
    )
    total_trend_df = benchmark_call(
        timings,
        "total_collisions_trend_over_time",
        total_collisions_trend_over_time,
        filtered_df,
    )

    forecast_df = (
        benchmark_call(
            timings,
            "forecast_collision_trend",
            forecast_collision_trend,
            total_trend_df,
            forecast_horizon,
        )
        if show_forecast
        else pd.DataFrame(columns=["date", "value", "series_type"])
    )
    if show_forecast and not total_trend_df.empty:
        if len(total_trend_df) < forecast_horizon:
            st.warning(
                "Forecast horizon is longer than available data. Results may be unreliable."
            )
    day_of_week_df = (
        benchmark_call(
            timings,
            "collisions_by_day_of_week",
            collisions_by_day_of_week,
            filtered_df,
        )
        if not filtered_df.empty
        else pd.DataFrame()
    )
    month_df = (
        benchmark_call(
            timings,
            "collisions_by_month",
            collisions_by_month,
            filtered_df,
        )
        if not filtered_df.empty
        else pd.DataFrame()
    )

    #st.subheader("Key Metrics")

    # Row 1: core executive KPIs
    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)

    total_collisions = len(filtered_df)
    fatal_count = (
        int((filtered_df["severity_class"] == "Fatal").sum())
        if "severity_class" in filtered_df.columns
        else 0
    )
    injury_count = (
        int((filtered_df["severity_class"] == "Injury").sum())
        if "severity_class" in filtered_df.columns
        else 0
    )
    property_damage_count = (
        int((filtered_df["severity_class"] == "Property Damage").sum())
        if "severity_class" in filtered_df.columns
        else 0
    )
    peak_hour = int(hourly_df.iloc[0]["OCC_HOUR"]) if not hourly_df.empty else "-"

    kpi1.metric("Total Collisions", total_collisions)
    kpi2.metric("Fatal", fatal_count)
    kpi3.metric("Injury", injury_count)
    kpi4.metric("Property Damage", property_damage_count)
    kpi5.metric("Peak Hour", peak_hour)
    kpi6.metric("Time Window", recent_days_option)

    # Row 2: road user exposure KPIs
    #st.subheader("Road Users Involved")
    r1, r2, r3 = st.columns(3)

    pedestrian_count = (
        int(filtered_df["PEDESTRIAN"].eq("YES").sum())
        if "PEDESTRIAN" in filtered_df.columns
        else 0
    )
    bicycle_count = (
        int(filtered_df["BICYCLE"].eq("YES").sum())
        if "BICYCLE" in filtered_df.columns
        else 0
    )
    motorcycle_count = (
        int(filtered_df["MOTORCYCLE"].eq("YES").sum())
        if "MOTORCYCLE" in filtered_df.columns
        else 0
    )

    r1.metric("Pedestrians", pedestrian_count)
    r2.metric("Bicycles", bicycle_count)
    r3.metric("Motorcycles", motorcycle_count)



    st.caption(
        f"Filters applied — Year: {selected_years or 'Latest'} | "
        f"Division: {selected_divisions or 'All'} | "
        f"Neighbourhood: {selected_neighbourhoods or 'All'} | "
        f"Severity: {collision_severity} | "
        f"Window: {recent_days_option}"
    )

    # Row 1: Trend
    col_chart, col_legend = st.columns([4, 1], gap="large")

    with col_chart:
        st.subheader("Collision Trend")
        #fritz
        if not trend_df.empty:
            end_chart = log_timed_block("st.altair_chart.trend")

            charts = []

            # 1. Existing severity trend (UNCHANGED)
            severity_chart = (
                alt.Chart(trend_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("value:Q", title="Collision Count"),
                    color=alt.Color(
                        "severity_type:N",
                        scale=alt.Scale(
                            domain=["Fatal", "Injury", "Property Damage"],
                            range=["#dc3545", "#ff8c00", "#ffc107"],
                        ),
                        legend=None,
                    ),
                    tooltip=["date:T", "severity_type:N", "value:Q"],
                )
            )
            charts.append(severity_chart)

            # 2. Total actual (BLUE solid)
            if not total_trend_df.empty:
                total_chart = (
                    alt.Chart(total_trend_df)
                    .mark_line(point=False, strokeWidth=3)
                    .encode(
                        x="date:T",
                        y="value:Q",
                        color=alt.value("#1f77b4"),
                        tooltip=["date:T", "value:Q"],
                    )
                )
                charts.append(total_chart)

            # 3. Forecast (BLUE dashed)
            if show_forecast and not forecast_df.empty:
                forecast_chart = (
                    alt.Chart(forecast_df)
                    .mark_line(point=False, strokeDash=[6, 4], strokeWidth=3)
                    .encode(
                        x="date:T",
                        y="value:Q",
                        color=alt.value("#1f77b4"),
                        tooltip=["date:T", "value:Q"],
                    )
                )
                charts.append(forecast_chart)

            st.altair_chart(alt.layer(*charts), use_container_width=True)
            
            end_chart()


            if show_forecast and forecast_df.empty:
                st.info("Not enough data available to generate a forecast.")
        else:
            st.info("No trend data available for the current filter selection.")
    with col_legend:
        components.html(
            """
            <br><br>
            <div style="font-family:sans-serif; font-size:14px; color:white; padding-top:8px;">

                <div style="font-weight:600; margin-bottom:8px;">Actual</div>

                <div style="display:flex; flex-direction:column; gap:8px; margin-bottom:14px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="width:30px; border-top:3px solid #dc3545;"></div>
                        <span>Fatal</span>
                    </div>

                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="width:30px; border-top:3px solid #ff8c00;"></div>
                        <span>Injury</span>
                    </div>

                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="width:30px; border-top:3px solid #ffc107;"></div>
                        <span>Property Damage</span>
                    </div>

                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="width:30px; border-top:3px solid #1f77b4;"></div>
                        <span>Total Collisions</span>
                    </div>
                </div>

                <div style="font-weight:600; margin-bottom:8px; margin-top:20px;">Forecast</div>

                <div style="display:flex; flex-direction:column; gap:8px; margin-bottom:14px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="width:30px; border-top:3px dashed #1f77b4;"></div>
                        <span>Total (Forecast)</span>
                    </div>
                </div>

                <!-- 👇 Explanation -->
                <div style="font-size:14px; color:#cccccc; line-height:1.4; margin-top:20px;">
                    Severity-level forecasting is not shown due to variability in low-frequency events (e.g., fatal collisions).
                </div>

            </div>
            """,
            height=380,
        )
    
    # Row 2: Hour + Neighbourhoods
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.subheader("Hourly Distribution")

        if not hourly_df.empty:
            end_chart = log_timed_block("st.line_chart.hourly")

            # Sort and rename for clean chart
            hourly_chart_df = hourly_df.sort_values("OCC_HOUR").copy()
            hourly_chart_df = hourly_chart_df.rename(
                columns={
                    "OCC_HOUR": "Hour",
                    "collision_count": "Collisions"
                }
            )

            # Line chart (Total only, blue)
            chart = (
                alt.Chart(hourly_chart_df)
                .mark_line(point=True, color=ChartConfig.TOTAL_COLOR)  # 🔵 Total
                .encode(
                    x=alt.X("Hour:Q", title="Hour of Day"),
                    y=alt.Y("Collisions:Q", title="Collision Count"),
                    tooltip=["Hour:Q", "Collisions:Q"],
                )
            )

            st.altair_chart(chart, use_container_width=True)

            end_chart()
        else:
            st.info("No hourly data available for the current filter selection.")

    with col2:
        st.subheader("Top High-Risk Neighbourhoods")
        if not neighbourhood_df.empty:
            end_df = log_timed_block("st.dataframe.neighbourhoods")
            st.dataframe(neighbourhood_df, use_container_width=True, hide_index=True, height=280)
            end_df()
        else:
            st.info("No neighbourhood data available for the current filter selection.")

    # Row 3: Day of Week + Month
    # Row 3: Day of Week + Month + Top Divisions
    col3, col4, col5 = st.columns(3, gap="large")

    # with col3:
    #     st.subheader("Week Day Distribution")
    #     if not day_of_week_df.empty:
    #         end_bar = log_timed_block("st.bar_chart.day_of_week")
    #         st.bar_chart(day_of_week_df.set_index("day_of_week")["collision_count"])
            
    #         end_bar()
    #     else:
    #         st.info("No day-of-week data available for the current filter selection.")
    with col3:
        st.subheader("Week Day Distribution")
        if not day_of_week_df.empty:
            end_chart = log_timed_block("st.altair_chart.day_of_week")

            weekday_chart_df = day_of_week_df.copy()
            weekday_chart_df = weekday_chart_df.rename(
                columns={
                    "day_of_week": "Day",
                    "collision_count": "Collisions"
                }
            )

            chart = (
                alt.Chart(weekday_chart_df)
                .mark_bar(color=ChartConfig.TOTAL_COLOR)
                #.mark_bar(color=ChartConfig.severity_color(Severity.FATAL))
                .encode(
                    x=alt.X("Day:N", title="Day of Week", sort=[
                        "Monday", "Tuesday", "Wednesday", "Thursday",
                        "Friday", "Saturday", "Sunday"
                    ],  axis=alt.Axis(labelAngle=340)),
                    y=alt.Y("Collisions:Q", title="Collision Count"),
                    tooltip=["Day:N", "Collisions:Q"],
                )
            )

            st.altair_chart(chart, use_container_width=True)
            end_chart()
        else:
            st.info("No day-of-week data available for the current filter selection.")
    # with col4:
    #     st.subheader("Monthly Distribution")
    #     if not month_df.empty:
    #         end_bar = log_timed_block("st.bar_chart.month")
    #         st.bar_chart(month_df.set_index("month_name")["collision_count"])
    #         end_bar()
    #     else:
    #         st.info("No monthly data available for the current filter selection.")
    with col4:
        st.subheader("Monthly Distribution")
        if not month_df.empty:
            end_chart = log_timed_block("st.altair_chart.month")

            month_chart_df = month_df.copy()
            month_chart_df = month_chart_df.rename(
                columns={
                    "month_name": "Month",
                    "collision_count": "Collisions"
                }
            )

            chart = (
                alt.Chart(month_chart_df)
                .mark_bar(color=ChartConfig.TOTAL_COLOR)
                .encode(
                    x=alt.X("Month:N", title="Month", sort=[
                        "January", "February", "March", "April", "May", "June",
                        "July", "August", "September", "October", "November", "December"
                    ], axis=alt.Axis(labelAngle=340)),
                    y=alt.Y("Collisions:Q", title="Collision Count"),
                    tooltip=["Month:N", "Collisions:Q"],
                )
            )

            st.altair_chart(chart, use_container_width=True)
            end_chart()
        else:
            st.info("No monthly data available for the current filter selection.")

    with col5:
        st.subheader("Top Divisions")
        if not division_df.empty:
            end_df = log_timed_block("st.dataframe.divisions")
            st.dataframe(
                division_df,
                use_container_width=True,
                hide_index=True,
                height=260,
            )
            end_df()
        else:
            st.info("No division data available for the current filter selection.")
    
    # Row 6: Map
    st.subheader("Map View")
    deck, map_error = benchmark_call(
        timings,
        "build_map",
        build_map,
        filtered_df,
        map_mode,
        map_style_option,
    )

    if map_error:
        st.info(map_error)
    else:
        end_pydeck = log_timed_block("st.pydeck_chart")
        st.pydeck_chart(deck)
        end_pydeck()
        render_map_legend()

    # Row 7: Export + Preview
    col7, col8 = st.columns([1, 3], gap="large")

    with col7:
        st.subheader("Export")

        end_mkdir = log_timed_block("OUTPUT_DIR.mkdir")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        end_mkdir()

        export_path = OUTPUT_DIR / "dashboard_filtered_export.csv"
        benchmark_call(
            timings,
            "export_results",
            export_results,
            filtered_df,
            str(export_path),
        )

        end_open = log_timed_block("open_export_file")
        with open(export_path, "rb") as f:
            csv_data = f.read()
        end_open()

        end_download = log_timed_block("st.download_button")
        st.download_button(
            "Download CSV",
            data=csv_data,
            file_name="dashboard_filtered_export.csv",
            mime="text/csv",
            use_container_width=False,
        )
        end_download()

    with col8:
        st.subheader("Filtered Data Preview")
        end_df = log_timed_block("st.dataframe.preview")
        st.dataframe(filtered_df.head(5), use_container_width=True, hide_index=True)
        end_df()

    total_elapsed = total_end()
    timings.append(
        {
            "Function": "main_total_runtime",
            "Execution Time (s)": round(total_elapsed, 6),
            "Execution Time (ms)": round(total_elapsed * 1000, 2),
        }
    )

    #with st.expander("Performance Benchmark"):
    #    st.dataframe(pd.DataFrame(timings), use_container_width=True, hide_index=True)

    #with st.expander("Benchmark Log File"):
    #    if LOG_FILE.exists():
    #        st.code(LOG_FILE.read_text(encoding="utf-8"), language="text")


if __name__ == "__main__":
    main()