import pandas as pd

from src.analytics import collisions_by_hour

from src.analytics import collisions_by_neighbourhood

from src.analytics import collision_severity_analysis

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

   


def test_collision_severity_analysis_returns_expected_rows():
    df = pd.DataFrame(
        {
            "FATALITIES": [1, 0],
            "INJURY_COLLISIONS": [True, False],
            "PD_COLLISIONS": [False, True],
        }
    )
    result = collision_severity_analysis(df)
    assert len(result) == 3