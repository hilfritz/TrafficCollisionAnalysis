# src/demo_data_loader.py
from .data_loader import load_dataset

def main():
    df = load_dataset("data/traffic_collisions.csv")
    print("Dataset loaded successfully.")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(df.head())

if __name__ == "__main__":
    main()