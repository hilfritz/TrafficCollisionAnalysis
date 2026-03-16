import matplotlib.pyplot as plt
import pandas as pd


def plot_road_user_involvement(road_user_df: pd.DataFrame):
    """
    Generate chart showing collisions by road user type.
    """

    if not {"INVTYPE", "collision_count"}.issubset(road_user_df.columns):
        raise ValueError("DataFrame must contain INVTYPE and collision_count")

    fig, ax = plt.subplots(figsize=(10,6))

    ax.bar(
        road_user_df["INVTYPE"],
        road_user_df["collision_count"]
    )

    ax.set_title("Collisions by Road User Type")
    ax.set_xlabel("Road User Type")
    ax.set_ylabel("Number of Collisions")

    plt.xticks(rotation=45)
    plt.tight_layout()

    return fig