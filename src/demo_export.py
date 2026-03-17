import pandas as pd
from src.analytics import export_results

# Example analysis result
data = pd.DataFrame({
    "road_user_type": ["Pedestrian", "Cyclist", "Automobile"],
    "collision_count": [120, 85, 300]
})

file_path = export_results(data, "output/road_user_summary.csv")

print("File exported to:", file_path)