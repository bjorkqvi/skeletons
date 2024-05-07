# skeletons

Geo-skeletons is an easy extendable way to build python classes to represent gridded and non-gridded geophysical data. It provides the basic structure to work with spherical and cartesian coordinates, and can be extended to data-specific objects by adding coordinates, data variables and logical masks. It also integrates with the geo-parameters module to provide easy access to metadata.

Please see https://data-skeletons.readthedocs.io/en/latest/ for a documentation.

For example, to create a python class representing wind data as x, and y components, but connencted magnitude and direction wrappers for convinience:
```python
from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_time, add_magnitude
import geo_parameters as gp
import pandas as pd


@add_magnitude(gp.wind.Wind("wind"), x="u", y="v", direction=gp.wind.WindDir("wdir"))
@add_datavar(gp.wind.YWind("v"))
@add_datavar(gp.wind.XWind("u"))
@add_time()
class Wind(GriddedSkeleton):
    pass

>> Wind.core

------------------------------ Coordinate groups -------------------------------
Spatial:    (y, x)
Grid:       (y, x)
Gridpoint:  (time)
All:        (time, y, x)
------------------------------------- Data -------------------------------------
Variables:
    u  (time, y, x):  0.0 [m/s] x_wind
    v  (time, y, x):  0.0 [m/s] y_wind
Masks:
    *empty*
Magnitudes:
  wind: magnitude of (u,v) [m/s] wind_speed
Directions:
  wdir: direction of (u,v) [deg] wind_from_direction
--------------------------------------------------------------------------------
```
To create an instance of this class, provide the coordinate values at initialization. The spatial coordinates can be either lon/lat or UTM x/y. Here we will use spherical coordinates, but set the spatial resolution to about 4 km.

```python
data = Wind(
    lon=(0, 10),
    lat=(60, 70),
    time=pd.date_range("2020-01-01 00:00", "2020-01-10 00:00", freq="1h"),
)
data.set_spacing(dm=4000)

>> data

<Wind (GriddedSkeleton)>
------------------------------ Coordinate groups -------------------------------
Spatial:    (lat, lon)
Grid:       (lat, lon)
Gridpoint:  (time)
All:        (time, lat, lon)
------------------------------------ Xarray ------------------------------------
<xarray.Dataset> Size: 5kB
Dimensions:  (time: 217, lat: 282, lon: 140)
Coordinates:
  * time     (time) datetime64[ns] 2kB 2020-01-01 ... 2020-01-10
  * lat      (lat) float64 2kB 60.0 60.04 60.07 60.11 ... 69.89 69.93 69.96 70.0
  * lon      (lon) float64 1kB 0.0 0.07194 0.1439 0.2158 ... 9.856 9.928 10.0
Data variables:
    *empty*
---------------------------------- Empty data ----------------------------------
Empty variables:
    u  (time, lat, lon):  0.0 [m/s] x_wind
    v  (time, lat, lon):  0.0 [m/s] y_wind
-------------------------- Magnitudes and directions ---------------------------
  wind: magnitude of (u,v) [m/s] wind_speed
  wdir: direction of (u,v) [deg] wind_from_direction
--------------------------------------------------------------------------------
```

As we can see, no data is yet stored in the underlying xarray Dataset. We can now set and get the data:
```python
>> data.set_u(3) # For non-constant value set numpy array
>> data.set_v(6)
```

The wind speed and direction can be retrieved and is calculated from the x,y-components:
```python
>> data.wind()
array([[[6.70820393, 6.70820393, 6.70820393, ..., 6.70820393,
         6.70820393, 6.70820393],
        [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393,
         6.70820393, 6.70820393],
        [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393,
         6.70820393, 6.70820393],
        ...,

>> data.wdir()
array([[[206.56505118, 206.56505118, 206.56505118, ..., 206.56505118,
         206.56505118, 206.56505118],
        [206.56505118, 206.56505118, 206.56505118, ..., 206.56505118,
         206.56505118, 206.56505118],
        [206.56505118, 206.56505118, 206.56505118, ..., 206.56505118,
         206.56505118, 206.56505118],
        ...,

>> data.wdir(dir_type='to')
array([[[26.56505118, 26.56505118, 26.56505118, ..., 26.56505118,
         26.56505118, 26.56505118],
        [26.56505118, 26.56505118, 26.56505118, ..., 26.56505118,
         26.56505118, 26.56505118],
        [26.56505118, 26.56505118, 26.56505118, ..., 26.56505118,
         26.56505118, 26.56505118],
        ...,
```

The direction (direction from) was parsed using the metadata in the gp.Wind.WindDir-parameters standard_name (wind_**from**_direction). The direction of the data can also be specified when setting data. 

For example, to set the wind direction that is in mathematical convention (radians, 0=east, pi/2=north). Here we set data that is transposed and has an extra trivial dimension, but it can be reshaped by providing information about non-trivial dimensions:
```python
# Defined over 'lat', 'lon', 'ensamble', 'time', instead of the 'time','lat','lon' that we want.
>> wind_data = np.full((282,140,1,217),0)
# We can ignore the trivial 'ensemble' dimension that we don't use
>> data.set_wdir(wind_data, coords=('lat','lon','time'), dir_type='math')

>> data
<Wind (GriddedSkeleton)>
------------------------------ Coordinate groups -------------------------------
Spatial:    (lat, lon)
Grid:       (lat, lon)
Gridpoint:  (time)
All:        (time, lat, lon)
------------------------------------ Xarray ------------------------------------
<xarray.Dataset> Size: 137MB
Dimensions:  (time: 217, lat: 282, lon: 140)
Coordinates:
  * time     (time) datetime64[ns] 2kB 2020-01-01 ... 2020-01-10
  * lat      (lat) float64 2kB 60.0 60.04 60.07 60.11 ... 69.89 69.93 69.96 70.0
  * lon      (lon) float64 1kB 0.0 0.07194 0.1439 0.2158 ... 9.856 9.928 10.0
Data variables:
    u        (time, lat, lon) float64 69MB 6.708 6.708 6.708 ... 6.708 6.708
    v        (time, lat, lon) float64 69MB 0.0 0.0 0.0 0.0 ... 0.0 0.0 0.0 0.0
-------------------------- Magnitudes and directions ---------------------------
  wind: magnitude of (u,v) [m/s] wind_speed
  wdir: direction of (u,v) [deg] wind_from_direction
--------------------------------------------------------------------------------
```

We can see that the wind speed is kept as it is and the direction is rotated in a way that correspons to westerly winds, and this is also what we get when the wind direction is retrieved:
```python
>> data.wdir()
array([[[270., 270., 270., ..., 270., 270., 270.],
        [270., 270., 270., ..., 270., 270., 270.],
        [270., 270., 270., ..., 270., 270., 270.],
        ...,
```
For large data the arrays can be stored as dask arrays. This can be activated for an instance:

```python
>> data.dask.activate()
>> data.u()
dask.array<array, shape=(217, 282, 140), dtype=float64, chunksize=(217, 282, 140), chunktype=numpy.ndarray>

>> data.u(dask=False)
array([[[6.70820393, 6.70820393, 6.70820393, ..., 6.70820393,
         6.70820393, 6.70820393],
        [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393,
         6.70820393, 6.70820393],
        [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393,
         6.70820393, 6.70820393],
        ...,

>> data.u(dask=False, data_array=True, lon=slice(0,1), time='2020-01-01 15:00')
<xarray.DataArray 'u' (lat: 282, lon: 14)> Size: 32kB
array([[6.70820393, 6.70820393, 6.70820393, ..., 6.70820393, 6.70820393,
        6.70820393],
       [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393, 6.70820393,
        6.70820393],
       [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393, 6.70820393,
        6.70820393],
       ...,
       [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393, 6.70820393,
        6.70820393],
       [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393, 6.70820393,
        6.70820393],
       [6.70820393, 6.70820393, 6.70820393, ..., 6.70820393, 6.70820393,
        6.70820393]])
Coordinates:
    time     datetime64[ns] 8B 2020-01-01T15:00:00
  * lat      (lat) float64 2kB 60.0 60.04 60.07 60.11 ... 69.89 69.93 69.96 70.0
  * lon      (lon) float64 112B 0.0 0.07194 0.1439 ... 0.7914 0.8633 0.9353
```

To have this as the default behavious for the class, just add the @activate_dask-decorator.
