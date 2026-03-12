# src/cleaning.py
import pandas as pd


# Placeholder neighbourhood values observed in the dataset.
# These represent unknown or unassigned locations and should
# be treated as missing values during cleaning.
PLACEHOLDER_NEIGHBOURHOODS = {"NSA", "", "UNKNOWN", "N/A", "NONE"}


# Columns that must be numeric for analysis.
# These fields are used in time analysis and spatial analysis.
NUMERIC_COLUMNS = ["OCC_YEAR", "OCC_HOUR", "LAT_WGS84", "LONG_WGS84"]


def convert_occ_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert OCC_DATE column to pandas datetime.

    This enables time-based analysis such as:
    - collisions by month
    - collisions by weekday
    - trend analysis over time
    """
    result = df.copy()

    # Convert values to datetime.
    # Invalid values will become NaT (Not a Time)
    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")

    return result


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert selected columns to numeric types.

    Some CSV datasets contain numbers stored as strings.
    This conversion ensures numeric operations can be performed.
    """
    result = df.copy()

    for col in NUMERIC_COLUMNS:

        # Convert values to numeric
        # Invalid values are converted to NaN
        result[col] = pd.to_numeric(result[col], errors="coerce")

    return result


def normalize_neighbourhood_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize neighbourhood column values.

    Placeholder values such as NSA or UNKNOWN represent
    missing geographic information and are converted to NaN.
    """
    result = df.copy()

    # Normalize text formatting
    result["NEIGHBOURHOOD_158"] = (
        result["NEIGHBOURHOOD_158"]
        .fillna("")              # Replace missing values temporarily
        .astype(str)             # Ensure string operations are possible
        .str.strip()             # Remove extra spaces
    )

    # Replace placeholder values with missing values
    result.loc[
        result["NEIGHBOURHOOD_158"].str.upper().isin(PLACEHOLDER_NEIGHBOURHOODS),
        "NEIGHBOURHOOD_158",
    ] = pd.NA

    return result


def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add data quality indicator columns.

    These flags allow analysts to identify problematic records
    without necessarily deleting them immediately.
    """
    result = df.copy()

    # Coordinates are considered valid if:
    # - latitude and longitude exist
    # - coordinates are not equal to 0
    result["HAS_VALID_COORDINATES"] = (
        result["LAT_WGS84"].notna()
        & result["LONG_WGS84"].notna()
        & (result["LAT_WGS84"] != 0)
        & (result["LONG_WGS84"] != 0)
    )

    # A valid neighbourhood exists if the column is not missing
    result["HAS_VALID_NEIGHBOURHOOD"] = result["NEIGHBOURHOOD_158"].notna()

    return result


def clean_collision_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize the Toronto traffic collision dataset.

    This function prepares raw collision records for analysis by applying
    a series of data cleaning and validation steps.

    Cleaning operations performed
    -------------------------------

    1. Remove duplicate records
       - Exact duplicate rows are removed to prevent double counting.

    2. Convert OCC_DATE to datetime
       - Converts the OCC_DATE column to pandas datetime format.
       - Invalid values are coerced to NaT.

    3. Convert numeric fields
       The following columns are converted to numeric types:
       - OCC_YEAR
       - OCC_HOUR
       - LAT_WGS84
       - LONG_WGS84

       Invalid values are coerced to NaN.

    4. Normalize neighbourhood values
       - Placeholder neighbourhood values such as:
         "NSA", "", "UNKNOWN", "N/A", "NONE"
         are converted to missing values (NaN).

    5. Add data quality flags
       Two boolean flags are added to help track data quality:

       HAS_VALID_COORDINATES
           True if LAT_WGS84 and LONG_WGS84 exist and are not equal to 0.

       HAS_VALID_NEIGHBOURHOOD
           True if NEIGHBOURHOOD_158 contains a valid value.

    6. Remove invalid coordinates
       - Rows with LAT_WGS84 or LONG_WGS84 equal to 0 are removed.
       - These rows cannot be used for spatial analysis.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw collision dataset loaded from CSV.

    Returns
    -------
    pandas.DataFrame
        Cleaned dataset ready for analytics and visualization.
    """

    result = df.copy()

    # Remove duplicate records to prevent double counting
    result = result.drop_duplicates()

    # Convert OCC_DATE to datetime format
    result = convert_occ_date(result)

    # Convert numeric columns used for analysis
    result = convert_numeric_columns(result)

    # Normalize placeholder neighbourhood values
    result = normalize_neighbourhood_values(result)

    # Add data quality flags
    result = add_quality_flags(result)

    # Remove rows with invalid geographic coordinates
    # because they cannot be used for spatial analysis
    result = result[result["HAS_VALID_COORDINATES"]].copy()

    return result


def print_data_quality_report(report: dict) -> None:
    """
    Print a formatted summary of dataset quality issues.
    """

    print("\nDATA QUALITY REPORT")
    print("---------------------------")
    print(f"Total rows: {report['total_rows']}")
    print(f"Duplicate rows: {report['duplicate_rows']}")
    print(f"Missing OCC_DATE: {report['missing_occ_date']}")
    print(f"Missing OCC_HOUR: {report['missing_occ_hour']}")
    print(f"Invalid coordinates: {report['invalid_coordinates']}")
    print(f"Placeholder neighbourhoods: {report['placeholder_neighbourhoods']}")
    print("---------------------------\n")


def generate_data_quality_report(df: pd.DataFrame) -> dict:
    """
    Generate a summary of potential data quality issues
    in the raw collision dataset before cleaning.

    This helps analysts understand dataset limitations.
    """

    report = {}

    # Total number of rows in the dataset
    report["total_rows"] = len(df)

    # Count duplicate rows
    report["duplicate_rows"] = int(df.duplicated().sum())

    # Count missing date values
    report["missing_occ_date"] = int(df["OCC_DATE"].isna().sum())

    # Count missing hour values
    report["missing_occ_hour"] = int(df["OCC_HOUR"].isna().sum())

    # Identify invalid coordinates
    # Coordinates equal to 0 or missing cannot be mapped geographically
    lat = pd.to_numeric(df["LAT_WGS84"], errors="coerce")
    lon = pd.to_numeric(df["LONG_WGS84"], errors="coerce")

    report["invalid_coordinates"] = int(
        ((lat == 0) | (lon == 0) | lat.isna() | lon.isna()).sum()
    )

    # Count placeholder or missing neighbourhood values
    report["placeholder_neighbourhoods"] = int(
        df["NEIGHBOURHOOD_158"]
        .fillna("")
        .astype(str)
        .str.upper()
        .isin(PLACEHOLDER_NEIGHBOURHOODS)
        .sum()
    )

    return report