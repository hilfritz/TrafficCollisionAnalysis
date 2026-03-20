from datetime import datetime
from pathlib import Path

import pandas as pd

from src.cleaning import clean_collision_data
from src.config import DEFAULT_DATASET_PATH
from src.data_loader import load_dataset
from src.analytics import (
    collisions_by_hour,
    collisions_by_neighbourhood,
    collision_severity_analysis,
    road_user_analysis,
)

README_FILE = Path("README.md")
PREPARED_DATASET_PATH = Path("data/processed/traffic_collisions_prepared.parquet")
OUTPUT_FILE = Path("outputs/readme_results_validation.txt")


def read_readme_results_section(readme_path: Path) -> str:
    if not readme_path.exists():
        return "README.md not found."

    text = readme_path.read_text(encoding="utf-8", errors="ignore")

    start_marker = "# Results"
    end_markers = [
        "# Dataset Schema Summary",
        "# TDD Workflow",
        "# User Stories",
    ]

    start_idx = text.find(start_marker)
    if start_idx == -1:
        return "Results section not found in README.md."

    end_idx = len(text)
    for marker in end_markers:
        marker_idx = text.find(marker, start_idx + len(start_marker))
        if marker_idx != -1:
            end_idx = min(end_idx, marker_idx)

    return text[start_idx:end_idx].strip()


def safe_peak_hour(hourly_df: pd.DataFrame) -> str:
    if hourly_df.empty:
        return "N/A"
    return f"{int(hourly_df.iloc[0]['OCC_HOUR']):02d}:00"


def dataframe_to_text(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if df.empty:
        return "No data available."
    if max_rows is not None:
        df = df.head(max_rows)
    return df.to_string(index=False)


def road_user_summary_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "road_user_type": ["Pedestrian", "Bicycle", "Motorcycle"],
            "collision_count": [
                int(df["PEDESTRIAN"].fillna("").astype(str).str.strip().str.upper().eq("YES").sum())
                if "PEDESTRIAN" in df.columns else 0,
                int(df["BICYCLE"].fillna("").astype(str).str.strip().str.upper().eq("YES").sum())
                if "BICYCLE" in df.columns else 0,
                int(df["MOTORCYCLE"].fillna("").astype(str).str.strip().str.upper().eq("YES").sum())
                if "MOTORCYCLE" in df.columns else 0,
            ],
        }
    )


def severity_summary_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    if "severity_class" not in df.columns:
        return pd.DataFrame(columns=["severity_type", "value"])

    return pd.DataFrame(
        {
            "severity_type": ["Fatal", "Injury", "Property Damage"],
            "value": [
                int((df["severity_class"] == "Fatal").sum()),
                int((df["severity_class"] == "Injury").sum()),
                int((df["severity_class"] == "Property Damage").sum()),
            ],
        }
    )


def validate_raw_cli_path() -> dict:
    raw_df = load_dataset(DEFAULT_DATASET_PATH)
    clean_df = clean_collision_data(raw_df)

    hourly_df = collisions_by_hour(clean_df)
    neighbourhood_df = collisions_by_neighbourhood(clean_df, top_n=10)

    # Uses current analytics.py behavior
    severity_df = collision_severity_analysis(clean_df)

    # Try analytics function first, fall back if needed
    try:
        road_user_df = road_user_analysis(clean_df)
    except Exception as exc:
        road_user_df = pd.DataFrame(
            {"road_user_type": ["ERROR"], "collision_count": [0]}
        )
        road_user_df["note"] = str(exc)

    return {
        "label": "RAW / CLI VALIDATION",
        "dataset_source": str(DEFAULT_DATASET_PATH),
        "row_count": len(clean_df),
        "peak_hour": safe_peak_hour(hourly_df),
        "hourly_df": hourly_df,
        "neighbourhood_df": neighbourhood_df,
        "severity_df": severity_df,
        "road_user_df": road_user_df,
    }


def validate_prepared_dashboard_path() -> dict:
    if not PREPARED_DATASET_PATH.exists():
        return {
            "label": "PREPARED / DASHBOARD VALIDATION",
            "dataset_source": str(PREPARED_DATASET_PATH),
            "row_count": 0,
            "peak_hour": "N/A",
            "hourly_df": pd.DataFrame(),
            "neighbourhood_df": pd.DataFrame(),
            "severity_df": pd.DataFrame(),
            "road_user_df": pd.DataFrame(),
            "note": "Prepared parquet not found.",
        }

    df = pd.read_parquet(PREPARED_DATASET_PATH)

    hourly_df = collisions_by_hour(df)
    neighbourhood_df = collisions_by_neighbourhood(df, top_n=10)
    severity_df = severity_summary_dashboard(df)
    road_user_df = road_user_summary_dashboard(df)

    return {
        "label": "PREPARED / DASHBOARD VALIDATION",
        "dataset_source": str(PREPARED_DATASET_PATH),
        "row_count": len(df),
        "peak_hour": safe_peak_hour(hourly_df),
        "hourly_df": hourly_df,
        "neighbourhood_df": neighbourhood_df,
        "severity_df": severity_df,
        "road_user_df": road_user_df,
    }


def section_lines(result: dict) -> list[str]:
    lines: list[str] = []
    lines.append(result["label"])
    lines.append("-" * 80)
    lines.append(f"Dataset source: {result['dataset_source']}")
    lines.append(f"Total Collisions: {result['row_count']}")
    lines.append(f"Peak Hour: {result['peak_hour']}")
    if "note" in result:
        lines.append(f"Note: {result['note']}")
    lines.append("")

    lines.append("TOP COLLISION HOURS")
    lines.append(dataframe_to_text(result["hourly_df"], max_rows=10))
    lines.append("")

    lines.append("HIGH RISK NEIGHBOURHOODS")
    lines.append(dataframe_to_text(result["neighbourhood_df"], max_rows=10))
    lines.append("")

    lines.append("COLLISION SEVERITY")
    lines.append(dataframe_to_text(result["severity_df"]))
    lines.append("")

    lines.append("ROAD USER ANALYSIS")
    lines.append(dataframe_to_text(result["road_user_df"]))
    lines.append("")

    return lines


def main() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    raw_result = validate_raw_cli_path()
    prepared_result = validate_prepared_dashboard_path()

    lines: list[str] = []
    lines.append("README RESULTS VALIDATION")
    lines.append("=" * 80)
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("README RESULTS SECTION")
    lines.append("-" * 80)
    lines.append(read_readme_results_section(README_FILE))
    lines.append("")

    lines.extend(section_lines(raw_result))
    lines.extend(section_lines(prepared_result))

    lines.append("COMPARISON NOTES")
    lines.append("-" * 80)
    lines.append(
        "RAW / CLI uses load_dataset + clean_collision_data + analytics.py functions."
    )
    lines.append(
        "PREPARED / DASHBOARD uses the prepared parquet and dashboard-style severity/road-user logic."
    )
    lines.append(
        "If severity or road-user counts differ, the dashboard/prepared path is usually the newer source of truth."
    )
    lines.append("")

    OUTPUT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"Validation file written to: {OUTPUT_FILE.resolve()}")


if __name__ == "__main__":
    main()