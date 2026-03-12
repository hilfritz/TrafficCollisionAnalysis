# tests/test_data_loader.py
from pathlib import Path

import pandas as pd
import pytest

from src.data_loader import load_dataset


REQUIRED_COLUMNS = [
    "EVENT_UNIQUE_ID",
    "OCC_DATE",
    "OCC_YEAR",
    "OCC_HOUR",
    "DIVISION",
    "FATALITIES",
    "INJURY_COLLISIONS",
    "PD_COLLISIONS",
    "NEIGHBOURHOOD_158",
    "LONG_WGS84",
    "LAT_WGS84",
    "AUTOMOBILE",
    "MOTORCYCLE",
    "BICYCLE",
    "PEDESTRIAN",
]


def test_load_dataset_returns_dataframe(tmp_path: Path):
    csv_file = tmp_path / "traffic_collisions.csv"

    df = pd.DataFrame(
        [
            {
                "EVENT_UNIQUE_ID": "1",
                "OCC_DATE": "2024-01-01",
                "OCC_YEAR": 2024,
                "OCC_HOUR": 8,
                "DIVISION": "D11",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "NO",
                "PD_COLLISIONS": "YES",
                "NEIGHBOURHOOD_158": "Yorkdale-Glen Park",
                "LONG_WGS84": -79.4,
                "LAT_WGS84": 43.7,
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "BICYCLE": "NO",
                "PEDESTRIAN": "NO",
            }
        ]
    )
    df.to_csv(csv_file, index=False)

    result = load_dataset(csv_file)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_load_dataset_raises_file_not_found_error_for_invalid_path():
    with pytest.raises(FileNotFoundError, match="Dataset not found"):
        load_dataset("does_not_exist.csv")


def test_load_dataset_raises_value_error_when_required_columns_missing(tmp_path: Path):
    csv_file = tmp_path / "traffic_collisions_missing_cols.csv"

    df = pd.DataFrame(
        [
            {
                "EVENT_UNIQUE_ID": "1",
                "OCC_DATE": "2024-01-01",
            }
        ]
    )
    df.to_csv(csv_file, index=False)

    with pytest.raises(ValueError, match="Missing required columns"):
        load_dataset(csv_file)


def test_load_dataset_contains_required_columns(tmp_path: Path):
    csv_file = tmp_path / "traffic_collisions.csv"

    df = pd.DataFrame(
        [
            {
                "EVENT_UNIQUE_ID": "1",
                "OCC_DATE": "2024-01-01",
                "OCC_YEAR": 2024,
                "OCC_HOUR": 8,
                "DIVISION": "D11",
                "FATALITIES": 0,
                "INJURY_COLLISIONS": "NO",
                "PD_COLLISIONS": "YES",
                "NEIGHBOURHOOD_158": "Yorkdale-Glen Park",
                "LONG_WGS84": -79.4,
                "LAT_WGS84": 43.7,
                "AUTOMOBILE": "YES",
                "MOTORCYCLE": "NO",
                "BICYCLE": "NO",
                "PEDESTRIAN": "NO",
            }
        ]
    )
    df.to_csv(csv_file, index=False)

    result = load_dataset(csv_file)

    for col in REQUIRED_COLUMNS:
        assert col in result.columns