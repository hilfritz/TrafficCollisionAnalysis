import pandas as pd

from src.analytics import collisions_by_hour


def test_collisions_by_hour_returns_hourly_counts():
    df = pd.DataFrame(
        {
            "OCC_HOUR": [8, 8, 9, 10, 10, 10]
        }
    )

    result = collisions_by_hour(df)

    assert list(result.columns) == ["OCC_HOUR", "collision_count"]
    assert len(result) == 3


def test_collisions_by_hour_counts_correctly():
    df = pd.DataFrame(
        {
            "OCC_HOUR": [8, 8, 9, 10, 10, 10]
        }
    )

    result = collisions_by_hour(df)

    assert int(result.iloc[0]["OCC_HOUR"]) == 10
    assert int(result.iloc[0]["collision_count"]) == 3


def test_collisions_by_hour_ignores_missing_hours():
    df = pd.DataFrame(
        {
            "OCC_HOUR": [8, 8, None, 10, None, 10]
        }
    )

    result = collisions_by_hour(df)

    assert result["OCC_HOUR"].isna().sum() == 0
    assert int(result["collision_count"].sum()) == 4


def test_collisions_by_hour_raises_error_if_column_missing():
    df = pd.DataFrame(
        {
            "NOT_OCC_HOUR": [8, 9, 10]
        }
    )

    try:
        collisions_by_hour(df)
        assert False, "Expected ValueError for missing OCC_HOUR column"
    except ValueError as e:
        assert "Missing required columns" in str(e)