import pandas as pd
from src.filtering import filter_by_neighbourhood
from src.analytics import filter_collisions

def test_filter_by_neighbourhood():
    data = pd.DataFrame({
        "Neighbourhood": ["A", "B", "A"]
    })

    result = filter_by_neighbourhood(data, "A")

    assert len(result) == 2


def test_filter_by_hour():

    data = pd.DataFrame({
        "OCC_HOUR": [1, 2, 2, 3],
        "OCC_YEAR": [2022, 2022, 2023, 2023]
    })

    result = filter_collisions(data, hours=[2])

    assert len(result) == 2
    assert all(result["OCC_HOUR"] == 2)


def test_filter_by_year():

    data = pd.DataFrame({
        "OCC_HOUR": [1, 2, 3],
        "OCC_YEAR": [2021, 2022, 2022]
    })

    result = filter_collisions(data, years=[2022])

    assert len(result) == 2
    assert all(result["OCC_YEAR"] == 2022)


def test_filter_by_division():
    data = pd.DataFrame({
        "OCC_YEAR": [2021, 2022, 2022],
        "DIVISION": ["D11", "D12", "D12"],
        "NEIGHBOURHOOD_158": ["A", "B", "C"],
    })

    result = filter_collisions(data, divisions=["D12"])

    assert len(result) == 2
    assert all(result["DIVISION"] == "D12")
