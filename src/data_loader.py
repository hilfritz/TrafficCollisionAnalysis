from pathlib import Path
import pandas as pd


# List of required columns expected in the Toronto collision dataset.
# These columns are required for later analysis steps such as:
# - time-based collision analysis
# - neighbourhood risk ranking
# - severity analysis
# - road user involvement analysis
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
    """
    Validate that all required columns exist in the dataset.

    This ensures the dataset structure matches what the analytics
    pipeline expects before any processing begins.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataset loaded from the CSV file.

    Raises
    ------
    ValueError
        If one or more required columns are missing.
    """

    # Identify columns that are missing from the dataset
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    # If any required column is missing, raise a clear error
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def load_dataset(file_path: str | Path) -> pd.DataFrame:
    """
    Load the Toronto traffic collision dataset from a CSV file.

    This function performs the following steps:
    1. Verifies the dataset file exists
    2. Loads the CSV file into a pandas DataFrame
    3. Validates that all required columns are present

    Parameters
    ----------
    file_path : str or pathlib.Path
        Path to the collision dataset CSV file.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the collision dataset.

    Raises
    ------
    FileNotFoundError
        If the dataset file does not exist.

    ValueError
        If the dataset is missing required columns.
    """

    # Convert input path to a Path object for safer file handling
    path = Path(file_path)

    # Check whether the dataset file exists
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    # Load the dataset using pandas
    df = pd.read_csv(path)

    # Validate dataset structure before returning it
    validate_required_columns(df)

    return df