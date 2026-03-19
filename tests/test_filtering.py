import pandas as pd
from src.analytics import filter_collisions


def test_filter_by_hour():

    data = pd.DataFrame({
        "OCC_HOUR": [1, 2, 2, 3],
        "YEAR": [2022, 2022, 2023, 2023]
    })

    result = filter_collisions(data, hour=2)

    assert len(result) == 2
    assert all(result["OCC_HOUR"] == 2)


def test_filter_by_year():

    data = pd.DataFrame({
        "OCC_HOUR": [1, 2, 3],
        "YEAR": [2021, 2022, 2022]
    })

    result = filter_collisions(data, year=2022)

    assert len(result) == 2
    assert all(result["YEAR"] == 2022)