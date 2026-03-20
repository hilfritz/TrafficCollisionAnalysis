def filter_by_neighbourhood(df, neighbourhood):
    return df[df["Neighbourhood"] == neighbourhood]
def filter_by_neighbourhood(df, neighbourhood):
    if "Neighbourhood" not in df.columns:
        raise ValueError("Column not found")

    return df[df["Neighbourhood"] == neighbourhood].copy()