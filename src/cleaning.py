# src/cleaning.py
import pandas as pd


PLACEHOLDER_NEIGHBOURHOODS = {"NSA", "", "UNKNOWN", "N/A", "NONE"}


def clean_collision_data(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    # Remove exact duplicate rows
    result = result.drop_duplicates()

    # Convert date column
    result["OCC_DATE"] = pd.to_datetime(result["OCC_DATE"], errors="coerce")

    # Convert numeric fields
    numeric_columns = ["OCC_YEAR", "OCC_HOUR", "LAT_WGS84", "LONG_WGS84"]
    for col in numeric_columns:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    # Normalize neighbourhood values
    result["NEIGHBOURHOOD_158"] = result["NEIGHBOURHOOD_158"].fillna("").astype(str).str.strip()
    result.loc[
        result["NEIGHBOURHOOD_158"].str.upper().isin(PLACEHOLDER_NEIGHBOURHOODS),
        "NEIGHBOURHOOD_158",
    ] = pd.NA

    # Add flags
    result["HAS_VALID_COORDINATES"] = (
        result["LAT_WGS84"].notna()
        & result["LONG_WGS84"].notna()
        & (result["LAT_WGS84"] != 0)
        & (result["LONG_WGS84"] != 0)
    )

    result["HAS_VALID_NEIGHBOURHOOD"] = result["NEIGHBOURHOOD_158"].notna()

    # Remove rows with invalid coordinates
    result = result[result["HAS_VALID_COORDINATES"]].copy()

    return result