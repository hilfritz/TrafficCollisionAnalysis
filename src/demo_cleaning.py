from .config import DEFAULT_DATASET_PATH
from .data_loader import load_dataset
from .cleaning import (
    clean_collision_data,
    generate_data_quality_report,
    print_data_quality_report,
)

#python -m src.demo_cleaning
def main():
    df = load_dataset(DEFAULT_DATASET_PATH)

    report = generate_data_quality_report(df)
    print_data_quality_report(report)

    cleaned_df = clean_collision_data(df)

    print("Cleaning completed successfully.")
    print(f"Original rows: {len(df)}")
    print(f"Cleaned rows: {len(cleaned_df)}")
    print(cleaned_df.head())


if __name__ == "__main__":
    main()