# src/demo_cleaning.py
from src.data_loader import load_dataset
from src.cleaning import clean_collision_data, generate_data_quality_report, print_data_quality_report

# python -m src.demo_cleaning
def main():
    df = load_dataset("data/traffic_collisions.csv")
    report = generate_data_quality_report(df)
    print_data_quality_report(report)
    
    cleaned_df = clean_collision_data(df)

    print("Cleaning completed successfully.")
    print(f"Original rows: {len(df)}")
    print(f"Cleaned rows: {len(cleaned_df)}")
    print(cleaned_df[["EVENT_UNIQUE_ID", "OCC_DATE", "OCC_YEAR", "OCC_HOUR", "NEIGHBOURHOOD_158", "HAS_VALID_COORDINATES", "HAS_VALID_NEIGHBOURHOOD"]].head())


if __name__ == "__main__":
    main()