import pandas as pd
from src.filtering import filter_by_neighbourhood

def test_filter_by_neighbourhood():
    data = pd.DataFrame({
        "Neighbourhood": ["A", "B", "A"]
    })

    result = filter_by_neighbourhood(data, "A")

    assert len(result) == 2