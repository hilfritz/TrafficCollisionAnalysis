from pathlib import Path


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

DEFAULT_DATASET_PATH = DATA_DIR / "traffic_collisions.csv"


from enum import Enum


class Severity(str, Enum):
    FATAL = "Fatal"
    INJURY = "Injury"
    PROPERTY_DAMAGE = "Property Damage"


class ChartConfig:
    TOTAL_COLOR = "#1f77b4"

    SEVERITY_COLORS = {
        Severity.FATAL: "#dc3545",
        Severity.INJURY: "#ff8c00",
        Severity.PROPERTY_DAMAGE: "#ffc107",
    }

    LINE_WIDTH = 2
    FORECAST_DASH = [5, 5]

    @classmethod
    def severity_color(cls, severity: Severity | str) -> str:
        """
        Safely get color for a severity value.
        Accepts either Enum or string.
        """
        if isinstance(severity, str):
            try:
                severity = Severity(severity)
            except ValueError:
                return cls.TOTAL_COLOR

        return cls.SEVERITY_COLORS.get(severity, cls.TOTAL_COLOR)

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