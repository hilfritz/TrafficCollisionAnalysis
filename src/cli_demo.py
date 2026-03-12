from .analytics import collisions_by_hour
from .cleaning import clean_collision_data
from .config import DEFAULT_DATASET_PATH
from .data_loader import load_dataset


def main() -> None:
    df = load_dataset(DEFAULT_DATASET_PATH)
    cleaned_df = clean_collision_data(df)

    hourly_df = collisions_by_hour(cleaned_df)

    print("\n=== COLLISIONS BY HOUR ===")
    print(hourly_df.head(10).to_string(index=False))

#python -m src.cli_demo
if __name__ == "__main__":
    main()