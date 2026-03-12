from pathlib import Path


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

DEFAULT_DATASET_PATH = DATA_DIR / "traffic_collisions.csv"


# Required dataset columns for analysis
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


# Placeholder neighbourhood values that should be treated as missing
PLACEHOLDER_NEIGHBOURHOODS = {"NSA", "", "UNKNOWN", "N/A", "NONE"}


# Columns that should be converted to numeric for analysis
NUMERIC_COLUMNS = ["OCC_YEAR", "OCC_HOUR", "LAT_WGS84", "LONG_WGS84"]