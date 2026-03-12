from .config import DEFAULT_DATASET_PATH
from .data_loader import load_dataset

#python -m src.demo_data_loader
def main():
    df = load_dataset(DEFAULT_DATASET_PATH)

    print("Dataset loaded successfully.")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(df.head())


if __name__ == "__main__":
    main()