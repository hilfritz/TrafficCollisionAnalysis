# tests/test_cleaning.py
from pathlib import Path

import pandas as pd

from src.cleaning import clean_collision_data

# RED PHASE command pytest tests/test_cleaning.py -v
def make_sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "EVENT_UNIQUE_ID": "1",
                "OCC_DATE": "2024-01-01",
                "OCC_YEAR": "2024",
                "OCC_HOUR": "8",
                "DIVISION": "D11",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "NO",
                "PD_COLLISIONS": "YES",
                "NEIGHBOURHOOD_158": "Yorkdale-Glen Park",
                "LONG_WGS84": "-79.4",
                "LAT_WGS84": "43.7",
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "BICYCLE": "NO",
                "PEDESTRIAN": "NO",
            },
            {
                "EVENT_UNIQUE_ID": "2",
                "OCC_DATE": "2024-01-02",
                "OCC_YEAR": "2024",
                "OCC_HOUR": "17",
                "DIVISION": "D12",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "YES",
                "PD_COLLISIONS": "NO",
                "NEIGHBOURHOOD_158": "NSA",
                "LONG_WGS84": "0",
                "LAT_WGS84": "0",
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "BICYCLE": "YES",
                "PEDESTRIAN": "NO",
            },
            {
                "EVENT_UNIQUE_ID": "1",
                "OCC_DATE": "2024-01-01",
                "OCC_YEAR": "2024",
                "OCC_HOUR": "8",
                "DIVISION": "D11",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "NO",
                "PD_COLLISIONS": "YES",
                "NEIGHBOURHOOD_158": "Yorkdale-Glen Park",
                "LONG_WGS84": "-79.4",
                "LAT_WGS84": "43.7",
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "BICYCLE": "NO",
                "PEDESTRIAN": "NO",
            },
            {
                "EVENT_UNIQUE_ID": "3",
                "OCC_DATE": None,
                "OCC_YEAR": None,
                "OCC_HOUR": None,
                "DIVISION": "D14",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "NO",
                "PD_COLLISIONS": "YES",
                "NEIGHBOURHOOD_158": "",
                "LONG_WGS84": "-79.2",
                "LAT_WGS84": "43.6",
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "BICYCLE": "NO",
                "PEDESTRIAN": "YES",
            },
        ]
    )


def test_clean_collision_data_converts_occ_date_to_datetime():
    df = make_sample_df()

    result = clean_collision_data(df)

    assert pd.api.types.is_datetime64_any_dtype(result["OCC_DATE"])


def test_clean_collision_data_converts_year_hour_and_coordinates_to_numeric():
    df = make_sample_df()

    result = clean_collision_data(df)

    assert pd.api.types.is_numeric_dtype(result["OCC_YEAR"])
    assert pd.api.types.is_numeric_dtype(result["OCC_HOUR"])
    assert pd.api.types.is_numeric_dtype(result["LAT_WGS84"])
    assert pd.api.types.is_numeric_dtype(result["LONG_WGS84"])


def test_clean_collision_data_removes_duplicate_rows():
    df = make_sample_df()

    result = clean_collision_data(df)

    assert result["EVENT_UNIQUE_ID"].tolist().count("1") == 1


def test_clean_collision_data_removes_invalid_coordinates():
    df = make_sample_df()

    result = clean_collision_data(df)

    assert not ((result["LAT_WGS84"] == 0) | (result["LONG_WGS84"] == 0)).any()


def test_clean_collision_data_handles_placeholder_neighbourhood_values():
    df = make_sample_df()

    result = clean_collision_data(df)

    assert "NSA" not in result["NEIGHBOURHOOD_158"].astype(str).tolist()


def test_clean_collision_data_adds_missing_data_flags():
    df = make_sample_df()

    result = clean_collision_data(df)

    assert "HAS_VALID_COORDINATES" in result.columns
    assert "HAS_VALID_NEIGHBOURHOOD" in result.columns


def test_clean_collision_data_flags_missing_neighbourhood_as_invalid():
    df = make_sample_df()

    result = clean_collision_data(df)

    row = result.loc[result["EVENT_UNIQUE_ID"] == "3"].iloc[0]
    assert bool(row["HAS_VALID_NEIGHBOURHOOD"]) is False