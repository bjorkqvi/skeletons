# skeletons

Skeletons is an easy extendable way to represent gridded and non-gridded geophysical data. It provides the basic structure to work with spherical and cartesian coordinates, and can be extended to data-specific objects by adding coordinates, data variables and logical masks.

For example, to create a python class representing a 3D grid of water temperature data:
```python
from skeletons import GriddedSkeleton
from skeletons.coordinate_factory import add_coord
from skeletons.datavar_factory import add_datavar
import numpy as np

@add_datavar(name="water_temperature", default_value=10.0)
@add_coord(name="z", grid_coord=True)
class TemperatureData(GriddedSkeleton):
    pass

grid = TemperatureData(lon=(5, 10), lat=(58, 62), z=(0, 100))
grid.set_spacing(dnmi=1) # Set horizontal spacing to 1 nautical mile
grid.set_z_spacing(dx=1) # Set vertical spacing to 1 meter

# Replace the default values with random temperature values
n1, n2, n3 = grid.size()
new_data = np.random.rand(n1, n2, n3)
grid.set_water_temperature(new_data)

# Replace all values in the grid with the mean surface temperature
mean_surface_temperature = np.mean(grid.water_temperature(z=0))
grid.set_water_temperature(mean_surface_temperature)

# Get a list of all horizontal points of cartesian coordinates in UTM zone 33 N
grid.set_utm((33,'N'))
x, y = grid.xy()

# Reset UTM zone to the best match determined at initialization (32, 'V')
grid.set_utm()
```

To create a non-gridded object with wave height time series data and initialize it with cartesian coordinates in the UTM zone 33, N:

```python
from skeletons import PointSkeleton
from skeletons.datavar_factory import add_datavar
from skeletons.coordinate_factory import add_time
import pandas as pd

@add_datavar(name="hs", default_value=0.0)
@add_time(name="time", grid_coord=False)
class WaveHeight(PointSkeleton):
    pass

data = WaveHeight(
    x=(165640, 180189, 283749),
    y=(6666593, 6766055, 6769393),
    time=pd.date_range("2020-01-01 00:00", "2020-01-31 23:00", freq="1H"),
)
data.set_utm((33, "N"))

lon, lat = data.lonlat() # Converts the UTM coordinates to spherical coordinates
data.time() # Get times as a DatetimeIndex
data.days(datetime=False) # Get all days as a list of strings in the format ['YYYY-MM-dd', ...]
data.hours(datetime=False, fmt="%Y%M%d %H00") # Hours the format ['YYYYMMdd HH00', ...]
```
