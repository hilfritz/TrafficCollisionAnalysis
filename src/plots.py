import matplotlib.pyplot as plt
import pandas as pd


def plot_collisions_by_hour(hourly_df: pd.DataFrame):
    """
    Generate chart: collisions by hour

    Returns:
        matplotlib.figure.Figure
    """

    if not {"OCC_HOUR", "collision_count"}.issubset(hourly_df.columns):
        raise ValueError("hourly_df must contain OCC_HOUR and collision_count")

    chart_df = hourly_df.sort_values("OCC_HOUR")

    fig, ax = plt.subplots(figsize=(10, 5))

    ax.bar(chart_df["OCC_HOUR"].astype(str), chart_df["collision_count"])

    ax.set_title("Collisions by Hour")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Collision Count")

    plt.tight_layout()

    return fig


def plot_top_neighbourhoods(neighbourhood_df: pd.DataFrame):
    """
    Generate chart: collisions by neighbourhood

    Returns:
        matplotlib.figure.Figure
    """

    if not {"NEIGHBOURHOOD_158", "collision_count"}.issubset(neighbourhood_df.columns):
        raise ValueError("neighbourhood_df must contain NEIGHBOURHOOD_158 and collision_count")

    chart_df = neighbourhood_df.sort_values("collision_count", ascending=True)

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.barh(chart_df["NEIGHBOURHOOD_158"], chart_df["collision_count"])

    ax.set_title("Top Neighbourhoods by Collision Count")
    ax.set_xlabel("Collision Count")
    ax.set_ylabel("Neighbourhood")

    plt.tight_layout()

    return fig