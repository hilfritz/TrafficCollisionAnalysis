from .analytics import collisions_by_hour, collisions_by_neighbourhood, collision_severity_analysis
from .cleaning import clean_collision_data
from .config import DEFAULT_DATASET_PATH
from .data_loader import load_dataset


def main() -> None:
    df = load_dataset(DEFAULT_DATASET_PATH)
    cleaned_df = clean_collision_data(df)

    hourly_df = collisions_by_hour(cleaned_df)
    neighbourhood_df = collisions_by_neighbourhood(cleaned_df, top_n=10)
    severity_df = collision_severity_analysis(cleaned_df)

    print("\n=== COLLISIONS BY HOUR ===")
    print(hourly_df.head(10).to_string(index=False))


    print("\n=== TOP NEIGHBOURHOODS ===")
    print(neighbourhood_df.to_string(index=False))

    print("\n=== COLLISION SEVERITY ANALYSIS ===")
    print(severity_df.to_string(index=False))

#python -m src.cli_demo
if __name__ == "__main__":
    main()