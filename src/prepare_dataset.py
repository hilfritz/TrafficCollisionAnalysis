# src/prepare_dataset.py
from pathlib import Path

import pandas as pd

from src.cleaning import clean_collision_data
from src.data_loader import load_dataset


RAW_DATASET_PATH = Path("data/traffic_collisions.csv")
PROCESSED_DATASET_PATH = Path("data/processed/traffic_collisions_prepared.parquet")


def normalize_yes_no_nr(series: pd.Series) -> pd.Series:
    """
    Normalize text values such as YES / NO / N/R / blank.
    """
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )


def classify_severity_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Vectorized severity classification:
    Fatal > Injury > Property Damage > Other
    """
    fatalities = pd.to_numeric(df["FATALITIES"], errors="coerce").fillna(0)
    has_injury = normalize_yes_no_nr(df["INJURY_COLLISIONS"]).eq("YES")
    has_property_damage = normalize_yes_no_nr(df["PD_COLLISIONS"]).eq("YES")

    severity = pd.Series("Other", index=df.index, dtype="object")
    severity.loc[has_property_damage] = "Property Damage"
    severity.loc[has_injury] = "Injury"
    severity.loc[fatalities > 0] = "Fatal"

    return severity


def prepare_dataset(
    raw_path: str | Path = RAW_DATASET_PATH,
    output_path: str | Path = PROCESSED_DATASET_PATH,
) -> Path:
    raw_path = Path(raw_path)
    output_path = Path(output_path)

    df = load_dataset(raw_path)
    df = clean_collision_data(df)

    # Ensure OCC_DATE is datetime
    df["OCC_DATE"] = pd.to_datetime(df["OCC_DATE"], errors="coerce")

    # Derived date columns
    df["OCC_DATE_STR"] = df["OCC_DATE"].dt.strftime("%Y-%m-%d")
    df["YEAR"] = df["OCC_DATE"].dt.year
    df["MONTH"] = df["OCC_DATE"].dt.month
    df["DAY"] = df["OCC_DATE"].dt.day

    # Normalize source categorical fields used by the dashboard
    df["INJURY_COLLISIONS"] = normalize_yes_no_nr(df["INJURY_COLLISIONS"])
    df["PD_COLLISIONS"] = normalize_yes_no_nr(df["PD_COLLISIONS"])
    df["PEDESTRIAN"] = normalize_yes_no_nr(df["PEDESTRIAN"])
    df["BICYCLE"] = normalize_yes_no_nr(df["BICYCLE"])
    df["MOTORCYCLE"] = normalize_yes_no_nr(df["MOTORCYCLE"])

    # Keep only numeric fatality flag as a stable boolean
    df["HAS_FATALITY"] = pd.to_numeric(df["FATALITIES"], errors="coerce").fillna(0) > 0

    # Severity source of truth
    df["severity_class"] = classify_severity_vectorized(df)

    # Precomputed cluster coordinates
    df["lat_cluster"] = pd.to_numeric(df["LAT_WGS84"], errors="coerce").round(3)
    df["lon_cluster"] = pd.to_numeric(df["LONG_WGS84"], errors="coerce").round(3)

    # Normalize common text filters
    if "DIVISION" in df.columns:
        df["DIVISION"] = df["DIVISION"].astype(str).str.strip()
    if "NEIGHBOURHOOD_158" in df.columns:
        df["NEIGHBOURHOOD_158"] = df["NEIGHBOURHOOD_158"].astype(str).str.strip()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    return output_path


def main() -> None:
    output_path = prepare_dataset()
    print(f"Prepared dataset written to: {output_path}")


if __name__ == "__main__":
    main()