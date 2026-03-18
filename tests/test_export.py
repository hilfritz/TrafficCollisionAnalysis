import os
import pandas as pd
from src.analytics import export_results


def test_export_results_creates_file(tmp_path):

    data = pd.DataFrame({
        "type": ["A", "B"],
        "count": [10, 20]
    })

    output_file = tmp_path / "test_export.csv"

    path = export_results(data, str(output_file))

    assert os.path.exists(path)