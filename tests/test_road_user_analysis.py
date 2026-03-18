import pandas as pd
from src.analytics import road_user_analysis
from src.plots import plot_road_user_involvement


def test_road_user_analysis_returns_dataframe():

    data = pd.DataFrame({
        "INVTYPE": ["Driver", "Pedestrian", "Cyclist", "Driver", None]
    })

    result = road_user_analysis(data)

    assert isinstance(result, pd.DataFrame)
    assert "collision_count" in result.columns


def test_plot_road_user_involvement_returns_figure():

    data = pd.DataFrame({
        "INVTYPE": ["Driver", "Pedestrian", "Cyclist"],
        "collision_count": [50, 20, 10]
    })

    fig = plot_road_user_involvement(data)

    assert fig is not None