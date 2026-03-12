from pathlib import Path

import pandas as pd


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


def validate_required_columns(df: pd.DataFrame) -> None:
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def load_dataset(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    df = pd.read_csv(path)
    validate_required_columns(df)
    return df