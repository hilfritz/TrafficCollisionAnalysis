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
    result["NEIGHBOURHOOD_158"] = result["NEIGHBOURHOOD_158"].fillna("").astype(str).str.strip()
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
    result = df.copy()

    result = result.drop_duplicates()
    result = convert_occ_date(result)
    result = convert_numeric_columns(result)
    result = normalize_neighbourhood_values(result)
    result = add_quality_flags(result)

    result = result[result["HAS_VALID_COORDINATES"]].copy()

    return result