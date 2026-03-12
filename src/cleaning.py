# src/cleaning.py
import pandas as pd


PLACEHOLDER_NEIGHBOURHOODS = {"NSA", "", "UNKNOWN", "N/A", "NONE"}
NUMERIC_COLUMNS = ["OCC_YEAR", "OCC_HOUR", "LAT_WGS84", "LONG_WGS84"]


def convert_occ_date(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")
    return result


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in NUMERIC_COLUMNS:
        result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


def normalize_neighbourhood_values(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    # Normalize placeholder neighbourhood values
    result["NEIGHBOURHOOD_158"] = result["NEIGHBOURHOOD_158"].fillna("").astype(str).str.strip()
    # Replace placeholder neighbourhood names with missing values
    result.loc[
        result["NEIGHBOURHOOD_158"].str.upper().isin(PLACEHOLDER_NEIGHBOURHOODS),
        "NEIGHBOURHOOD_158",
    ] = pd.NA
    return result


def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["HAS_VALID_COORDINATES"] = (
        result["LAT_WGS84"].notna()
        & result["LONG_WGS84"].notna()
        & (result["LAT_WGS84"] != 0)
        & (result["LONG_WGS84"] != 0)
    )
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
    result = add_quality_flags(result)

    result = result[result["HAS_VALID_COORDINATES"]].copy()

    return result

def print_data_quality_report(report: dict) -> None:
    """
    Print a formatted data quality report.
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
    Generate a data quality summary for the collision dataset.

    This function inspects the raw dataset and reports common
    data quality issues before cleaning is applied.

    Checks performed
    ----------------
    - Total rows
    - Duplicate rows
    - Missing OCC_DATE values
    - Missing OCC_HOUR values
    - Invalid coordinates (LAT_WGS84 or LONG_WGS84 equal to 0)
    - Missing or placeholder neighbourhood values

    Returns
    -------
    dict
        Dictionary containing data quality metrics.
    """

    report = {}

    report["total_rows"] = len(df)

    report["duplicate_rows"] = df.duplicated().sum()

    report["missing_occ_date"] = df["OCC_DATE"].isna().sum()

    report["missing_occ_hour"] = df["OCC_HOUR"].isna().sum()

    report["invalid_coordinates"] = (
        ((df["LAT_WGS84"] == 0) | (df["LONG_WGS84"] == 0)).sum()
    )

    placeholder_values = {"NSA", "", "UNKNOWN", "N/A", "NONE"}

    report["placeholder_neighbourhoods"] = (
        df["NEIGHBOURHOOD_158"]
        .fillna("")
        .astype(str)
        .str.upper()
        .isin(placeholder_values)
        .sum()
    )

    return report