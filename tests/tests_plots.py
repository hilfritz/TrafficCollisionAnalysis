import pandas as pd
from src.plots import plot_collisions_by_hour, plot_top_neighbourhoods


def test_plot_collisions_by_hour_returns_figure():

    data = pd.DataFrame({
        "OCC_HOUR": [1, 2, 3],
        "collision_count": [10, 20, 15]
    })

    fig = plot_collisions_by_hour(data)

    assert fig is not None
    assert fig.__class__.__name__ == "Figure"


def test_plot_neighbourhood_returns_figure():

    data = pd.DataFrame({
        "NEIGHBOURHOOD_158": ["Downtown", "Scarborough", "North York"],
        "collision_count": [50, 30, 20]
    })

    fig = plot_top_neighbourhoods(data)

    assert fig is not None
    assert fig.__class__.__name__ == "Figure"