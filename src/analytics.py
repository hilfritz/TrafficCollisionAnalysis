import pandas as pd


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

    # Remove rows where OCC_HOUR is missing before grouping
    result = (
        df.dropna(subset=["OCC_HOUR"])
        .groupby("OCC_HOUR")
        .size()
        .reset_index(name="collision_count")
        .sort_values(["collision_count", "OCC_HOUR"], ascending=[False, True])
        .reset_index(drop=True)
    )

    return result