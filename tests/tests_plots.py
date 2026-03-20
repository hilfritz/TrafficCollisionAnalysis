import pandas as pd
import pytest
from src.plots import plot_collisions_by_hour, plot_top_neighbourhoods, plot_collision_severity


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


def test_plot_collision_severity_bar_and_pie_return_figures():

    data = pd.DataFrame({
        "severity_type": ["Fatalities", "Injury Collisions", "Property Damage Collisions"],
        "value": [1, 5, 20],
    })

    fig_bar = plot_collision_severity(data, kind="bar")
    assert fig_bar is not None
    assert fig_bar.__class__.__name__ == "Figure"

    fig_pie = plot_collision_severity(data, kind="pie")
    assert fig_pie is not None
    assert fig_pie.__class__.__name__ == "Figure"


def test_plot_collision_severity_invalid_inputs_raise():
    data = pd.DataFrame({"severity_type": ["A"], "value": [1]})

    with pytest.raises(ValueError):
        plot_collision_severity(data.drop(columns=["value"]))

    with pytest.raises(ValueError):
        plot_collision_severity(data, kind="unsupported")
    