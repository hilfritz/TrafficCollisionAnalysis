# src/app.py
import pandas as pd
import pydeck as pdk
import streamlit as st
from pathlib import Path
from time import perf_counter
from datetime import datetime
import streamlit.components.v1 as components
from src.config import OUTPUT_DIR

DEFAULT_PREPARED_DATASET_PATH = Path("data/processed/traffic_collisions_prepared.parquet")
LOG_FILE = Path("logs/app_benchmark.log")

import altair as alt
severity_colors = {
    "Fatal": "#dc3545",
    "Injury": "#ff8c00",
    "Property Damage": "#ffc107",
    "Other": "#6c757d",
}


def reset_log():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text("", encoding="utf-8")


def log_message(message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_timed_block(name: str):
    log_message(f"{name} START")
    start = perf_counter()

    def end():
        elapsed = perf_counter() - start
        elapsed_ms = elapsed * 1000
        log_message(f"{name} END took {elapsed:.6f}s ({elapsed_ms:.2f} ms)")
        return elapsed

    return end


def benchmark_call(timings: list[dict], func_name: str, func, *args, **kwargs):
    end_log = log_timed_block(func_name)
    result = func(*args, **kwargs)
    elapsed = end_log()
    timings.append(
        {
            "Function": func_name,
            "Execution Time (s)": round(elapsed, 6),
            "Execution Time (ms)": round(elapsed * 1000, 2),
        }
    )
    return result


@st.cache_data
def get_prepared_data(dataset_path: str):
    end_log = log_timed_block("get_prepared_data")
    df = pd.read_parquet(dataset_path)
    end_log()
    return df


def apply_collision_severity_filter(df, severity: str):
    end_log = log_timed_block("apply_collision_severity_filter")

    result = df.copy()

    if severity == "All":
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


def apply_recent_days_filter(df, recent_days: int | None):
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
):
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


def collisions_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("collisions_by_hour")
    if "OCC_HOUR" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["OCC_HOUR", "collision_count"])

    result = (
        df.dropna(subset=["OCC_HOUR"])
        .groupby("OCC_HOUR")
        .size()
        .reset_index(name="collision_count")
        .sort_values(["collision_count", "OCC_HOUR"], ascending=[False, True])
        .reset_index(drop=True)
    )
    end_log()
    return result


def collisions_by_neighbourhood(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    end_log = log_timed_block("collisions_by_neighbourhood")
    if "NEIGHBOURHOOD_158" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["NEIGHBOURHOOD_158", "collision_count"])

    result = (
        df.dropna(subset=["NEIGHBOURHOOD_158"])
        .groupby("NEIGHBOURHOOD_158")
        .size()
        .reset_index(name="collision_count")
        .sort_values("collision_count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    end_log()
    return result


def collision_severity_analysis(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("collision_severity_analysis")

    if "severity_class" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["severity_type", "value"])

    result = pd.DataFrame(
        {
            "severity_type": [
                "Fatal",
                "Injury",
                "Property Damage",
            ],
            "value": [
                int((df["severity_class"] == "Fatal").sum()),
                int((df["severity_class"] == "Injury").sum()),
                int((df["severity_class"] == "Property Damage").sum()),
            ],
        }
    )

    end_log()
    return result


def road_user_analysis(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("road_user_analysis")

    result = pd.DataFrame(
        {
            "road_user_type": ["Pedestrian", "Bicycle", "Motorcycle"],
            "collision_count": [
                int(df["PEDESTRIAN"].eq("YES").sum()) if "PEDESTRIAN" in df.columns else 0,
                int(df["BICYCLE"].eq("YES").sum()) if "BICYCLE" in df.columns else 0,
                int(df["MOTORCYCLE"].eq("YES").sum()) if "MOTORCYCLE" in df.columns else 0,
            ],
        }
    )

    end_log()
    return result


def collisions_trend_over_time(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("collisions_trend_over_time")

    if "OCC_DATE" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["date", "collision_count"])

    result = df.copy()
    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")
    result = result.dropna(subset=["OCC_DATE"])

    if result.empty:
        end_log()
        return pd.DataFrame(columns=["date", "collision_count"])

    trend_df = (
        result.groupby(result["OCC_DATE"].dt.date)
        .size()
        .reset_index(name="collision_count")
        .rename(columns={"OCC_DATE": "date"})
    )
    trend_df["date"] = pd.to_datetime(trend_df["date"])
    trend_df = trend_df.sort_values("date").reset_index(drop=True)

    end_log()
    return trend_df


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

def severity_trend_over_time(df: pd.DataFrame, selected_severity: str) -> pd.DataFrame:
    end_log = log_timed_block("severity_trend_over_time")

    if "OCC_DATE" not in df.columns or "severity_class" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["date", "severity_type", "value"])

    result = df.copy()
    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")
    result = result.dropna(subset=["OCC_DATE"])

    if result.empty:
        end_log()
        return pd.DataFrame(columns=["date", "severity_type", "value"])

    severity_order = ["Fatal", "Injury", "Property Damage"]

    if selected_severity == "All":
        result = result[result["severity_class"].isin(severity_order)].copy()
    elif selected_severity in severity_order:
        result = result[result["severity_class"] == selected_severity].copy()
    else:
        end_log()
        return pd.DataFrame(columns=["date", "severity_type", "value"])

    if result.empty:
        end_log()
        return pd.DataFrame(columns=["date", "severity_type", "value"])

    trend_df = (
        result.groupby([result["OCC_DATE"].dt.date, "severity_class"])
        .size()
        .reset_index(name="value")
        .rename(columns={"OCC_DATE": "date", "severity_class": "severity_type"})
    )

    trend_df["date"] = pd.to_datetime(trend_df["date"])
    trend_df = trend_df.sort_values(["date", "severity_type"]).reset_index(drop=True)

    end_log()
    return trend_df

def render_map_legend():
    end_log = log_timed_block("render_map_legend")
    components.html(
        """
    <div style="padding:5px 5px; border:1px solid #ddd; border-radius:8px; background-color:#fafafa; margin-top:8px;">
    

    <div style="display:flex; align-items:center; gap:20px; flex-wrap:wrap;">
        <b>Legend</b>    
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
        height=150,
    )
    end_log()


def build_map(filtered_df, map_mode: str, map_style_option: str):
    end_build_map = log_timed_block("build_map")
    log_message(f"MAP VIEW selected: {map_mode}")

    map_df = build_severity_map_dataframe(filtered_df)
    #st.write(map_df["severity_class"].value_counts())

    if map_df.empty:
        end_build_map()
        return None, "No valid coordinates available for the current filter selection."

    end_sample = log_timed_block("build_map.sample_limit")
    if len(map_df) > 6000:
        map_df = map_df.sample(6000, random_state=42)
    end_sample()

    end_center = log_timed_block("build_map.compute_center")
    center_lat = map_df["LAT_WGS84"].mean()
    center_lon = map_df["LONG_WGS84"].mean()
    end_center()

    color_map = get_severity_color_map()
    layers = []

    if map_mode == "Point Map":
        end_mode = log_timed_block("build_map.point_layers")
        severity_draw_order = ["Other", "Property Damage", "Injury", "Fatal"]
        for severity in severity_draw_order:
            color = color_map[severity]
        #for severity, color in color_map.items():
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

    elif map_mode == "Heatmap":
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

    elif map_mode == "Cluster Bubbles":
        cluster_df = build_cluster_dataframe(map_df)

        if cluster_df.empty:
            end_build_map()
            return None, "No cluster data available for the current filter selection."

        end_mode = log_timed_block("build_map.cluster_layers")
        for severity, color in color_map.items():
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

    if map_mode == "Cluster Bubbles":
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
    lat_range = map_df["LAT_WGS84"].max() - map_df["LAT_WGS84"].min()
    lon_range = map_df["LONG_WGS84"].max() - map_df["LONG_WGS84"].min()
    spread = max(lat_range, lon_range)

    if spread < 0.01:
        zoom = 14
    elif spread < 0.05:
        zoom = 13
    elif spread < 0.1:
        zoom = 12
    elif spread < 0.5:
        zoom = 11
    else:
        zoom = 10

    
    deck = pdk.Deck(
        layers=layers,
        # initial_view_state=pdk.ViewState(
        #     latitude=center_lat,
        #     longitude=center_lon,
        #     zoom=10,
        #     pitch=0,
        # ),
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

def apply_road_user_filter(
    df,
    pedestrian: bool = False,
    bicycle: bool = False,
    motorcycle: bool = False,
):
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

def main() -> None:
    reset_log()
    timings = []

    total_end = log_timed_block("main_total_runtime")

    st.set_page_config(page_title="Toronto Collision Analysis", layout="wide")

    st.title("Toronto Collision Analysis")
    #st.caption(
    #    "Executive view of collision volume, severity, road-user exposure, and spatial risk patterns."
    #)

    dataset_path = st.sidebar.text_input("Prepared dataset path", str(DEFAULT_PREPARED_DATASET_PATH))

    try:
        df = benchmark_call(timings, "get_prepared_data", get_prepared_data, dataset_path)
    except Exception as exc:
        st.error(f"Failed to load prepared dataset: {exc}")
        return

    st.sidebar.header("Filters")

    # =========================
    # 1. TIME WINDOW (FIRST)
    # =========================
    recent_days_option = st.sidebar.selectbox(
        "Time Window",
        ["All Time", "30 Days", "60 Days", "90 Days", "180 Days", "365 Days"],
        index=3,  # default 90 days
    )
    log_message(f"TREND WINDOW selected: {recent_days_option}")

    recent_days_lookup = {
        "All Time": None,
        "30 Days": 30,
        "60 Days": 60,
        "90 Days": 90,
        "180 Days": 180,
        "365 Days": 365,
    }
    recent_days = recent_days_lookup[recent_days_option]

    # =========================
    # 2. YEAR (SECOND)
    # =========================
    end_years = log_timed_block("sidebar.years")
    years = (
        sorted([int(y) for y in df["YEAR"].dropna().unique().tolist()])
        if "YEAR" in df.columns
        else []
    )
    default_year = [max(years)] if years else []

    selected_years = st.sidebar.multiselect(
        "Year",
        years,
        default=default_year,
    )
    end_years()

    # =========================
    # 3. DIVISION
    # =========================
    end_divisions = log_timed_block("sidebar.divisions")
    divisions = (
        sorted(df["DIVISION"].dropna().astype(str).unique().tolist())
        if "DIVISION" in df.columns
        else []
    )
    selected_divisions = st.sidebar.multiselect("Division", divisions)
    end_divisions()

    # =========================
    # 4. NEIGHBOURHOOD
    # =========================
    end_neigh = log_timed_block("sidebar.neighbourhoods")
    neighbourhoods = (
        sorted(df["NEIGHBOURHOOD_158"].dropna().astype(str).unique().tolist())
        if "NEIGHBOURHOOD_158" in df.columns
        else []
    )
    selected_neighbourhoods = st.sidebar.multiselect("Neighbourhood", neighbourhoods)
    end_neigh()

    # =========================
    # 5. COLLISION SEVERITY
    # =========================
    collision_severity = st.sidebar.selectbox(
        "Collision Severity",
        [
            "All",
            "Fatal",
            "Injury",
            "Property Damage",
            "Other",
        ],
    )
    log_message(f"COLLISION SEVERITY selected: {collision_severity}")

    # =========================
    # 6. ROAD USER
    # =========================
    st.sidebar.markdown("Involved Road User")

    filter_pedestrian = st.sidebar.checkbox("Pedestrian")
    filter_bicycle = st.sidebar.checkbox("Bicycle")
    filter_motorcycle = st.sidebar.checkbox("Motorcycle")

    selected_road_users = {
        "Pedestrian": filter_pedestrian,
        "Bicycle": filter_bicycle,
        "Motorcycle": filter_motorcycle,
    }
    log_message(f"ROAD USER FILTERS selected: {selected_road_users}")

    # =========================
    # 7. MAP VIEW
    # =========================
    map_mode = st.sidebar.radio(
        "Map View",
        ["Point Map", "Heatmap", "Cluster Bubbles"],
    )
    log_message(f"MAP MODE selected in sidebar: {map_mode}")

    # =========================
    # 8. MAP STYLE
    # =========================
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

    # =========================
    # FILTER PIPELINE (unchanged)
    # =========================
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

    # everything below stays the same

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
            top_n=10,
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

    #st.subheader("Key Metrics")
    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    total_collisions = len(filtered_df)
    total_fatalities = int(filtered_df["HAS_FATALITY"].sum()) if "HAS_FATALITY" in filtered_df.columns else 0
    total_injury_collisions = int((filtered_df["severity_class"] == "Injury").sum()) if "severity_class" in filtered_df.columns else 0
    peak_hour = int(hourly_df.iloc[0]["OCC_HOUR"]) if not hourly_df.empty else "-"

    kpi1.metric("Total Collisions", total_collisions)
    kpi2.metric("Fatalities", total_fatalities)
    kpi3.metric("Injury Collisions", total_injury_collisions)
    kpi4.metric("Peak Hour", peak_hour)
    kpi5.metric("Time Window", recent_days_option)

    # Row 1: full-width trend
    st.subheader("Collision Trend")
    if not trend_df.empty:
        end_chart = log_timed_block("st.altair_chart.trend")

        trend_chart = (
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
                    legend=alt.Legend(title="Severity"),
                ),
                tooltip=["date:T", "severity_type:N", "value:Q"],
            )
        )

        st.altair_chart(trend_chart, use_container_width=True)
        end_chart()
    else:
        st.info("No trend data available for the current filter selection.")

    # Row 2: hourly distribution + top neighbourhoods
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.subheader("Collision Distribution by Hour")
        if not hourly_df.empty:
            end_bar = log_timed_block("st.bar_chart.hourly")
            st.bar_chart(
                hourly_df.sort_values("OCC_HOUR").set_index("OCC_HOUR")["collision_count"]
            )
            end_bar()
        else:
            st.info("No hourly data available for the current filter selection.")

    with col2:
        st.subheader("Top High-Risk Neighbourhoods")
        if not neighbourhood_df.empty:
            end_df = log_timed_block("st.dataframe.neighbourhoods")
            st.dataframe(neighbourhood_df, use_container_width=True, hide_index=True)
            end_df()
        else:
            st.info("No neighbourhood data available for the current filter selection.")

    # Row 3: severity KPI cards + road exposure KPI cards
    col3, col4 = st.columns(2, gap="large")

    with col3:
        st.subheader("Severity Breakdown")
        if not severity_df.empty:
            fatal_value = int(
                severity_df.loc[severity_df["severity_type"] == "Fatal", "value"].iloc[0]
            )
            injury_value = int(
                severity_df.loc[severity_df["severity_type"] == "Injury", "value"].iloc[0]
            )
            property_damage_value = int(
                severity_df.loc[
                    severity_df["severity_type"] == "Property Damage", "value"
                ].iloc[0]
            )

            s1, s2, s3 = st.columns(3)
            s1.metric("Fatal", fatal_value)
            s2.metric("Injury", injury_value)
            s3.metric("Property Damage", property_damage_value)

            end_df = log_timed_block("st.dataframe.severity")
            st.dataframe(severity_df, use_container_width=True, hide_index=True)
            end_df()
        else:
            st.info("No severity data available for the current filter selection.")

    with col4:
        st.subheader("Road User Exposure")
        if not road_users_df.empty:
            pedestrian_value = int(
                road_users_df.loc[road_users_df["road_user_type"] == "Pedestrian", "collision_count"].iloc[0]
            )
            bicycle_value = int(
                road_users_df.loc[road_users_df["road_user_type"] == "Bicycle", "collision_count"].iloc[0]
            )
            motorcycle_value = int(
                road_users_df.loc[road_users_df["road_user_type"] == "Motorcycle", "collision_count"].iloc[0]
            )

            r1, r2, r3 = st.columns(3)
            r1.metric("Pedestrian", pedestrian_value)
            r2.metric("Bicycle", bicycle_value)
            r3.metric("Motorcycle", motorcycle_value)

            end_df = log_timed_block("st.dataframe.road_users")
            st.dataframe(road_users_df, use_container_width=True, hide_index=True)
            end_df()
        else:
            st.info("No road user data available for the current filter selection.")

    # Row 4: full-width map
    st.subheader("Map of Collisions")
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

    # Row 5: compact export + preview
    col5, col6 = st.columns([1, 3], gap="large")

    with col5:
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

    with col6:
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

    with st.expander("Performance Benchmark"):
        st.dataframe(pd.DataFrame(timings), use_container_width=True, hide_index=True)

    with st.expander("Benchmark Log File"):
        if LOG_FILE.exists():
            st.code(LOG_FILE.read_text(encoding="utf-8"), language="text")

if __name__ == "__main__":
    main()