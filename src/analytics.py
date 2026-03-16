import pandas as pd


def _require_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def collisions_by_hour(df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(df, ["OCC_HOUR"])
    return (
        df.dropna(subset=["OCC_HOUR"])
        .groupby("OCC_HOUR")
        .size()
        .reset_index(name="collision_count")
        .sort_values(["collision_count", "OCC_HOUR"], ascending=[False, True])
        .reset_index(drop=True)
    )


def collisions_by_neighbourhood(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    _require_columns(df, ["NEIGHBOURHOOD_158"])
    return (
        df.dropna(subset=["NEIGHBOURHOOD_158"])
        .groupby("NEIGHBOURHOOD_158")
        .size()
        .reset_index(name="collision_count")
        .sort_values("collision_count", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )


def collision_severity_analysis(df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(df, ["FATALITIES", "INJURY_COLLISIONS", "PD_COLLISIONS"])
    fatalities = int(pd.to_numeric(df["FATALITIES"], errors="coerce").fillna(0).sum())
    injury_collisions = int(df["INJURY_COLLISIONS"].fillna(False).astype(bool).sum())
    property_damage_collisions = int(df["PD_COLLISIONS"].fillna(False).astype(bool).sum())
    return pd.DataFrame(
        {
            "severity_type": ["Fatalities", "Injury Collisions", "Property Damage Collisions"],
            "value": [fatalities, injury_collisions, property_damage_collisions],
        }
    )


def road_user_analysis(df: pd.DataFrame) -> pd.DataFrame:
    _require_columns(df, ["PEDESTRIAN", "BICYCLE", "MOTORCYCLE", "AUTOMOBILE"])
    return pd.DataFrame(
        {
            "road_user_type": ["Pedestrian", "Bicycle", "Motorcycle", "Automobile"],
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
    divisions: list[str] | None = None,
    neighbourhoods: list[str] | None = None,
) -> pd.DataFrame:
    result = df.copy()
    if years:
        result = result[result["OCC_YEAR"].isin(years)]
    if divisions:
        result = result[result["DIVISION"].isin(divisions)]
    if neighbourhoods:
        result = result[result["NEIGHBOURHOOD_158"].isin(neighbourhoods)]
    return result.copy()


def export_results(df: pd.DataFrame, output_path: str) -> str:
    df.to_csv(output_path, index=False)
    return output_path

import pandas as pd

def road_user_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze collisions by road user type.

    Parameters
    ----------
    df : pandas.DataFrame
        Collision dataset

    Returns
    -------
    pandas.DataFrame
        Summary table of collisions by road user type
    """

    if "INVTYPE" not in df.columns:
        raise ValueError("Dataset must contain INVTYPE column")

    # Handle missing values
    df["INVTYPE"] = df["INVTYPE"].fillna("Unknown")

    # Count collisions by road user type
    summary = (
        df.groupby("INVTYPE")
        .size()
        .reset_index(name="collision_count")
        .sort_values("collision_count", ascending=False)
    )

    return summary
import pandas as pd


def filter_collisions(df: pd.DataFrame, hour=None, year=None):
    """
    Filter collision dataset by hour or year.

    Parameters
    ----------
    df : pandas.DataFrame
        Collision dataset
    hour : int (optional)
        Hour of day filter
    year : int (optional)
        Year filter

    Returns
    -------
    pandas.DataFrame
        Filtered dataset
    """

    filtered_df = df.copy()

    # Filter by hour
    if hour is not None:
        if "OCC_HOUR" not in filtered_df.columns:
            raise ValueError("Dataset must contain OCC_HOUR column")

        filtered_df = filtered_df[filtered_df["OCC_HOUR"] == hour]

    # Filter by year
    if year is not None:
        if "YEAR" not in filtered_df.columns:
            raise ValueError("Dataset must contain YEAR column")

        filtered_df = filtered_df[filtered_df["YEAR"] == year]

    return filtered_df