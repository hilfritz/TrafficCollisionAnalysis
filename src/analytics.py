# src/analytics.py
import pandas as pd
import os
from src.common import log_timed_block, reset_log, log_message, benchmark_call
from statsmodels.tsa.holtwinters import ExponentialSmoothing

def _require_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    """
    Validate that required columns exist before analysis.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataset.
    required_columns : list[str]
        Columns required for the analysis.

    Raises
    ------
    ValueError
        If one or more required columns are missing.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def collisions_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate collisions by OCC_HOUR to identify peak collision periods.

    This function supports time-based analysis by summarizing how many
    collisions occurred in each hour of the day.

    Cleaning assumptions:
    - rows with missing OCC_HOUR are excluded
    - results are sorted by highest collision count first
    - ties are sorted by OCC_HOUR ascending

    Parameters
    ----------
    df : pandas.DataFrame
        Cleaned collision dataset.

    Returns
    -------
    pandas.DataFrame
        DataFrame with:
        - OCC_HOUR
        - collision_count
    """
    _require_columns(df, ["OCC_HOUR"])

    result = (
        df.dropna(subset=["OCC_HOUR"])
        .groupby("OCC_HOUR")
        .size()
        .reset_index(name="collision_count")
        .sort_values(["collision_count", "OCC_HOUR"], ascending=[False, True])
        .reset_index(drop=True)
    )

    return result


def collisions_by_neighbourhood(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Rank neighbourhoods by collision frequency.

    This function supports location-based safety analysis by identifying
    which neighbourhoods have the highest number of recorded collisions.

    Cleaning assumptions:
    - rows with missing NEIGHBOURHOOD_158 are excluded
    - results are sorted by highest collision count first
    - output can be limited to the top_n neighbourhoods

    Parameters
    ----------
    df : pandas.DataFrame
        Cleaned collision dataset.
    top_n : int, default=10
        Number of top neighbourhoods to return.

    Returns
    -------
    pandas.DataFrame
        DataFrame with:
        - NEIGHBOURHOOD_158
        - collision_count
    """
    end_log = log_timed_block("collisions_by_neighbourhood")
    _require_columns(df, ["NEIGHBOURHOOD_158"])

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
    """
    Summarize fatalities, injury collisions, and property damage collisions.
    """
    end_log = log_timed_block("collision_severity_analysis")

    if "severity_class" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["severity_type", "value"])

    result = pd.DataFrame(
        {
            "severity_type": ["Fatal", "Injury", "Property Damage"],
            "value": [
                int((df["severity_class"] == "Fatal").sum()),
                int((df["severity_class"] == "Injury").sum()),
                int((df["severity_class"] == "Property Damage").sum()),
            ],
        }
    )

    end_log()
    return result

def collisions_by_division(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    end_log = log_timed_block("collisions_by_division")
    if "DIVISION" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["DIVISION", "collision_count"])

    result = (
        df.dropna(subset=["DIVISION"])
        .groupby("DIVISION")
        .size()
        .reset_index(name="collision_count")
        .sort_values("collision_count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    end_log()
    return result


def road_user_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count collisions involving key road user categories.
    """
    _require_columns(df, ["PEDESTRIAN", "BICYCLE", "MOTORCYCLE", "AUTOMOBILE"])

    return pd.DataFrame(
        {
            "road_user_type": [
                "Pedestrian",
                "Bicycle",
                "Motorcycle",
                "Automobile",
            ],
            "collision_count": [
                int(df["PEDESTRIAN"].fillna(False).astype(bool).sum()),
                int(df["BICYCLE"].fillna(False).astype(bool).sum()),
                int(df["MOTORCYCLE"].fillna(False).astype(bool).sum()),
                int(df["AUTOMOBILE"].fillna(False).astype(bool).sum()),
            ],
        }
    )
def filter_collisions(
    df: pd.DataFrame,
    years: list[int] | None = None,
    hours: list[int] | None = None,
    divisions: list[str] | None = None,
    neighbourhoods: list[str] | None = None,
) -> pd.DataFrame:
    """
    Filter the cleaned collision dataset by selected fields.
    """
    result = df.copy()

    if years:
        result = result[result["OCC_YEAR"].isin(years)]

    if hours:
        result = result[result["OCC_HOUR"].isin(hours)]

    if divisions:
        result = result[result["DIVISION"].isin(divisions)]

    if neighbourhoods:
        result = result[result["NEIGHBOURHOOD_158"].isin(neighbourhoods)]

    return result.copy()

def filter_collisionsold(
    df: pd.DataFrame,
    years: list[int] | None = None,
    divisions: list[str] | None = None,
    neighbourhoods: list[str] | None = None,
) -> pd.DataFrame:
    """
    Filter the cleaned collision dataset by selected fields.
    """
    result = df.copy()

    if years:
        result = result[result["OCC_YEAR"].isin(years)]

    if divisions:
        result = result[result["DIVISION"].isin(divisions)]

    if neighbourhoods:
        result = result[result["NEIGHBOURHOOD_158"].isin(neighbourhoods)]

    return result.copy()


def export_results(df: pd.DataFrame, output_path: str) -> str:
    """
    Export analysis results to a CSV file.

    Parameters
    ----------
    df : pandas.DataFrame
        Analysis results
    output_path : str
        Path to output CSV file

    Returns
    -------
    str
        Path to created file
    """

    # Create folder if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save CSV
    df.to_csv(output_path, index=False)

    return output_path


def collisions_by_day_of_week(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("collisions_by_day_of_week")

    if "OCC_DOW" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["day_of_week", "collision_count"])

    order = [
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday"
    ]

    result = (
        df.dropna(subset=["OCC_DOW"])
        .groupby("OCC_DOW")
        .size()
        .reset_index(name="collision_count")
        .rename(columns={"OCC_DOW": "day_of_week"})
    )

    result["day_of_week"] = pd.Categorical(
        result["day_of_week"],
        categories=order,
        ordered=True,
    )
    result = result.sort_values("day_of_week").reset_index(drop=True)

    end_log()
    return result

def collisions_by_month(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("collisions_by_month")

    if "MONTH" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["month_name", "collision_count"])

    month_lookup = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }

    result = df.copy()
    result["MONTH"] = pd.to_numeric(result["MONTH"], errors="coerce")
    result = result.dropna(subset=["MONTH"])

    if result.empty:
        end_log()
        return pd.DataFrame(columns=["month_name", "collision_count"])

    result["MONTH"] = result["MONTH"].astype(int)

    result = (
        result.groupby("MONTH")
        .size()
        .reset_index(name="collision_count")
        .rename(columns={"MONTH": "month"})
        .sort_values("month")
        .reset_index(drop=True)
    )

    result["month_name"] = result["month"].map(month_lookup)
    result["month_name"] = pd.Categorical(
        result["month_name"],
        categories=[
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ],
        ordered=True,
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

    if selected_severity == "All Severities":
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

def total_collisions_trend_over_time(df: pd.DataFrame) -> pd.DataFrame:
    end_log = log_timed_block("total_collisions_trend_over_time")

    if "OCC_DATE" not in df.columns:
        end_log()
        return pd.DataFrame(columns=["date", "value"])

    result = df.copy()
    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")
    result = result.dropna(subset=["OCC_DATE"])

    if result.empty:
        end_log()
        return pd.DataFrame(columns=["date", "value"])

    trend_df = (
        result.groupby(result["OCC_DATE"].dt.date)
        .size()
        .reset_index(name="value")
        .rename(columns={"OCC_DATE": "date"})
    )

    trend_df["date"] = pd.to_datetime(trend_df["date"])
    trend_df = trend_df.sort_values("date").reset_index(drop=True)

    end_log()
    return trend_df


def forecast_collision_trend(
    trend_df: pd.DataFrame,
    horizon_days: int = 30,
) -> pd.DataFrame:
    end_log = log_timed_block("forecast_collision_trend")

    if trend_df.empty or len(trend_df) < 30:
        end_log()
        return pd.DataFrame(columns=["date", "value", "series_type"])

    data = trend_df.copy()
    data = data.sort_values("date").reset_index(drop=True)
    data = data.set_index("date").asfreq("D")
    data["value"] = data["value"].fillna(0)

    if len(data) < 30:
        end_log()
        return pd.DataFrame(columns=["date", "value", "series_type"])

    try:
        model = ExponentialSmoothing(
            data["value"],
            trend="add",
            seasonal=None,
            initialization_method="estimated",
        )
        fitted = model.fit(optimized=True)
        forecast_values = fitted.forecast(horizon_days)
    except Exception:
        end_log()
        return pd.DataFrame(columns=["date", "value", "series_type"])

    forecast_df = forecast_values.reset_index()
    forecast_df.columns = ["date", "value"]
    forecast_df["series_type"] = "Forecast"

    end_log()
    return forecast_df