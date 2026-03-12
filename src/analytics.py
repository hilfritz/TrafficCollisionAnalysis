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

import pandas as pd

from src.analytics import collisions_by_neighbourhood


def test_collisions_by_neighbourhood_returns_grouped_counts():
    df = pd.DataFrame(
        {
            "NEIGHBOURHOOD_158": ["A", "A", "B", "C", "C", "C"]
        }
    )

    result = collisions_by_neighbourhood(df)

    assert list(result.columns) == ["NEIGHBOURHOOD_158", "collision_count"]
    assert len(result) == 3


def test_collisions_by_neighbourhood_sorts_by_collision_count_desc():
    df = pd.DataFrame(
        {
            "NEIGHBOURHOOD_158": ["A", "A", "B", "C", "C", "C"]
        }
    )

    result = collisions_by_neighbourhood(df)

    assert result.iloc[0]["NEIGHBOURHOOD_158"] == "C"
    assert int(result.iloc[0]["collision_count"]) == 3


def test_collisions_by_neighbourhood_ignores_missing_values():
    df = pd.DataFrame(
        {
            "NEIGHBOURHOOD_158": ["A", None, "B", "B", None, "C"]
        }
    )

    result = collisions_by_neighbourhood(df)

    assert result["NEIGHBOURHOOD_158"].isna().sum() == 0
    assert int(result["collision_count"].sum()) == 4


def test_collisions_by_neighbourhood_returns_top_n_only():
    df = pd.DataFrame(
        {
            "NEIGHBOURHOOD_158": ["A", "A", "B", "C", "C", "C", "D"]
        }
    )

    result = collisions_by_neighbourhood(df, top_n=2)

    assert len(result) == 2
    assert list(result["NEIGHBOURHOOD_158"]) == ["C", "A"]


def test_collisions_by_neighbourhood_raises_error_if_column_missing():
    df = pd.DataFrame(
        {
            "DIVISION": ["D11", "D12", "D14"]
        }
    )

    try:
        collisions_by_neighbourhood(df)
        assert False, "Expected ValueError for missing NEIGHBOURHOOD_158 column"
    except ValueError as e:
        assert "Missing required columns" in str(e)