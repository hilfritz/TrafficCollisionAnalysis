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


def plot_collision_severity(severity_df: pd.DataFrame, kind: str = "bar"):
    """
    Generate chart for collision severity analysis.

    Parameters
    ----------
    severity_df : pandas.DataFrame
        DataFrame with columns `severity_type` and `value` as returned by
        `collision_severity_analysis`.
    kind : str, optional
        Chart type: "bar" (default) or "pie".

    Returns
    -------
    matplotlib.figure.Figure
    """
    if not {"severity_type", "value"}.issubset(severity_df.columns):
        raise ValueError("severity_df must contain severity_type and value")

    fig, ax = plt.subplots(figsize=(8, 5))

    if kind == "bar":
        colors = ["#d9534f", "#f0ad4e", "#5bc0de"]
        ax.bar(severity_df["severity_type"], severity_df["value"], color=colors[: len(severity_df)])
        ax.set_title("Collision Severity Analysis")
        ax.set_ylabel("Count")
        ax.set_xlabel("Severity Type")
    elif kind == "pie":
        ax.pie(severity_df["value"], labels=severity_df["severity_type"], autopct="%1.1f%%", startangle=140)
        ax.set_title("Collision Severity Distribution")
    else:
        raise ValueError('kind must be "bar" or "pie"')

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