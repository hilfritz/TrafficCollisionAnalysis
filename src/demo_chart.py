import pandas as pd
from src.plots import plot_collisions_by_hour

data = pd.DataFrame({
    "OCC_HOUR": [1,2,3,4],
    "collision_count": [5,12,7,9]
})

fig = plot_collisions_by_hour(data)

fig.show()