Welcome to skeletons's documentation!
=====================================

**Geo-skeletons** is an easy extendable way to build python classes to represent gridded and non-gridded geophysical data. It provides the basic structure to work with spherical and cartesian coordinates, and can be extended to data-specific objects by adding coordinates, data variables and logical masks. It also integrates with the geo-parameters module to provide easy access to metadata.

Quick Installation
=============================================
To get started with geo-skeletons, you can install it with pip or conda:

.. code-block:: shell

   $ pip install geo-skeletons

.. code-block:: shell

   $ conda install -c conda-forge geo-skeletons

Using PointSkeletons
=============================================

Creation
---------------------------------------------

A point cloud can easily be represented as an unstructured skeleton:

.. code-block:: python

  from geo_skeletons import PointSkeleton
  points = PointSkeleton(lon=(30.0,30.1,30.5), lat=(60.0,60.0,60.8))


Accessing coordinates
---------------------------------------------

Coordinates are now accessible both in the (native) spherical and the (non-native) cartesian versions, where the best UTM-zone has automatically been deduced for the given lon/lat coordinates:

.. code-block:: python

  >>> points.lon()
  array([30. , 30.1, 30.5])
  >>> points.x()
  array([332705.17887694, 338279.24910909, 363958.72298911])

  >>> points.utm.zone()
  (36, 'V')

  >>> points.x(utm=(33,'W'))
  array([1331808.13859715, 1337286.99102854, 1338117.44887216])

  >>> points.utm.set((33,'W'))
  Setting UTM (33, 'W')

  >>> points.x()
  array([1331808.13859715, 1337286.99102854, 1338117.44887216])


Both methods have a ``strict`` option that only returns the coordinates if matches the native standard for the skeleton and returns ``None`` otherwise:

.. code-block:: python

  >>> points.lon(strict=True)
  array([30. , 30.1, 30.5])
  >>> points.x(strict=True)
  >>>

To get the native coordinates you can use the ``native`` option in either method:

.. code-block:: python

  >>> points.lon(native=True)
  array([30. , 30.1, 30.5])
  >>> points.x(native=True)
  array([30. , 30.1, 30.5])

The ``strict`` and ``native`` options are implemented to make it easier to use skeletons inside larger modules, since it removes the need for a lot of checks.

The .lonlat() and .xy() methods gives a tuple with arrays of coordinates:

.. code-block:: python

  >>> points.lonlat()
  (array([30. , 30.1, 30.5]), array([60. , 60. , 60.8]))


Underlying xarray Dataset structure
--------------------------------------------

The skeleton information is stored in an xarray Dataset. This will be convenient when the skeleton is expanded by additional coordinates or variables:

.. code-block:: python

  >>> points
  <PointSkeleton (Skeleton)>
  ------------------------------ Coordinate groups -------------------------------
  Spatial:    (inds)
  Grid:       (inds)
  Gridpoint:  *empty*
  All:        (inds)
  ------------------------------------ Xarray ------------------------------------
  <xarray.Dataset> Size: 72B
  Dimensions:  (inds: 3)
  Coordinates:
    * inds     (inds) int64 24B 0 1 2
  Data variables:
      lat      (inds) float64 24B 60.0 60.0 60.8
      lon      (inds) float64 24B 30.0 30.1 30.5
  Attributes:
      name:      LonelySkeleton
      utm_zone:  36V
  --------------------------------------------------------------------------------


  >>> points.ds()
  <xarray.Dataset> Size: 72B
  Dimensions:  (inds: 3)
  Coordinates:
    * inds     (inds) int64 24B 0 1 2
  Data variables:
      lat      (inds) float64 24B 60.0 60.0 60.8
      lon      (inds) float64 24B 30.0 30.1 30.5
  Attributes:
      name:      LonelySkeleton
      utm_zone:  36V

Since there is no gridded structure, these vectors are given as a function of indeces:

.. code-block:: python

  >>> points.inds()
  array([0, 1, 2])

The size of the skeleton, defined by the indeces, is given by:

.. code-block:: python

  >>> points.size()
  (3,)

However, the size of the *x- and y-vectors* are given by:

.. code-block:: python

  >>> points.nx()
  3
  >>> points.ny()
  3

The core of the Skeleton will still keep track of lon/lat as coordinates (to differentiate from actual data variables that can be added)

.. code-block:: python
  >>> points.core.data_vars() # 'lon' and 'lat' not included, since they are not "proper" data variables
  []
  >>> points.core.coords()
  ['inds']
  >>> points.core.coords('init') # A list of coordinates that are needed to initialize this class
  ['lat', 'lon']


Using GriddedSkeletons
=============================================

Creation and setting spacing
---------------------------------------------

Unlike a PointSkeleton, a GriddedSkeleton is defined on an area:

.. code-block:: python

  from geo_skeletons import GriddedSkeleton
  grid = GriddedSkeleton(lon=(30.0,30.5), lat=(60.0,60.8))


A structure can be given gy setting a desired spacing. The basic method is to specify the number of grid points in each direction:

.. code-block:: python

  grid.set_spacing(nx=6, ny=9)
  
The spacing can also be set by defining a longitude/latitude spacing, and appoximate spacing in metres, or an approximate spacing in nautical miles:

.. code-block:: python

  grid.set_spacing(dlon=0.1, dlat=0.1)
  grid.set_spacing(dx=6000, dy=8000) # 6 km resolution in longitude and 8 km resolution in latitude direction
  grid.set_spacing(dm=8000) # Same as dx=dy=dm
  grid.set_spacing(dnmi=0.5) # Half a nautical mile spacing

Since the grid has been defined by the edges, the desired spacing can sometimes only be approximated:

.. code-block:: python

  >>> grid.set_spacing(dlon=0.024, dlat=0.09)
  >>> grid.dlon()
  0.023809523809523808
  >>> grid.dlat()
  0.08888888888888857

If setting an exact spacing is more important than preserving the exact area, then this can be forced, and the area is changed slightly instead:

.. code-block:: python

  >>> grid = GriddedSkeleton(lon=(30.0,30.5), lat=(60.0,60.8))
  
  >>> grid.edges('lon')
  (30.0, 30.5)
  >>> grid.edges('lat')
  (60.0, 60.8)

  >>> grid.set_spacing(dlon=0.024, dlat=0.09, floating_edge=True)
  >>> grid.dlon()
  0.024000000000000063
  >>> grid.dlat()
  0.09000000000000025
  
  >>> grid.edges('lon')
  (30.0, 30.504)
  >>> grid.edges('lat')
  (60.0, 60.81)


Accessing the coordinates
---------------------------------------------

Setting the spacing creates longitude an latitude vectors:

.. code-block:: python

  >>> grid.set_spacing(nx=6, ny=9)
  >>> grid.lon()
  array([30. , 30.1, 30.2, 30.3, 30.4, 30.5])
  >>> grid.lat()
  array([60. , 60.1, 60.2, 60.3, 60.4, 60.5, 60.6, 60.7, 60.8])


Note, that these methods gives the vectors defining the grid, **not** the longitude and latitude coordinates of ALL the points (as for the PointSkeleton). Nonetheless, the ``.lonlat()`` method can be used:

.. code-block:: python

  >>> grid.lonlat()
  (array([30. , 30.1, 30.2, 30.3, 30.4, 30.5, 30. , 30.1, 30.2, 30.3, 30.4,
         30.5, 30. , 30.1, 30.2, 30.3, 30.4, 30.5, 30. , 30.1, 30.2, 30.3,
         30.4, 30.5, 30. , 30.1, 30.2, 30.3, 30.4, 30.5, 30. , 30.1, 30.2,
         30.3, 30.4, 30.5, 30. , 30.1, 30.2, 30.3, 30.4, 30.5, 30. , 30.1,
         30.2, 30.3, 30.4, 30.5, 30. , 30.1, 30.2, 30.3, 30.4, 30.5]), array([60. , 60. , 60. , 60. , 60. , 60. , 60.1, 60.1, 60.1, 60.1, 60.1,
         60.1, 60.2, 60.2, 60.2, 60.2, 60.2, 60.2, 60.3, 60.3, 60.3, 60.3,
         60.3, 60.3, 60.4, 60.4, 60.4, 60.4, 60.4, 60.4, 60.5, 60.5, 60.5,
         60.5, 60.5, 60.5, 60.6, 60.6, 60.6, 60.6, 60.6, 60.6, 60.7, 60.7,
         60.7, 60.7, 60.7, 60.7, 60.8, 60.8, 60.8, 60.8, 60.8, 60.8]))

Therefore, a list of coordinates for all the points (regardless of which type of skeleton you have) can always be retrieved as:

.. code-block:: python

  lon, lat = grid.lonlat()

A longitude grid (meshgrid) can be retrieved both in spherical and cartesian coordinates:

.. code-block:: python

  >>> grid.longrid()
  array([[30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5],
         [30. , 30.1, 30.2, 30.3, 30.4, 30.5]])

  >>> grid.xgrid()
  array([[332705.17887694, 338279.24910909, 343853.56603089,
          349428.12110114, 355002.90578231, 360577.91154036],
         [333210.54871541, 338767.76600796, 344325.23072873,
          349882.93431135, 355440.86819304, 360999.02381454],
         [333716.43160159, 339256.77897873, 344797.37450988,
          350338.20960391, 355879.27567328, 361420.56413389],
         [334222.82599447, 339746.28653178, 345269.9959361 ,
          350793.94559198, 356318.12688754, 361842.53121431],
         [334729.73035137, 340236.28717586, 345743.09356756,
          351250.14088716, 356757.42049886, 362264.92377027],
         [335237.14312789, 340726.77941807, 346216.66596286,
          351706.79409952, 357197.15516879, 362687.74051482],
         [335745.06277796, 341217.76176387, 346690.711679  ,
          352163.90383761, 357637.32955744, 363110.98015957],
         [336253.48775383, 341709.2327171 , 347165.2292714 ,
          352621.46870845, 358077.9423234 , 363534.64141473],
         [336762.41650608, 342201.19077999, 347640.21729394,
          353079.48731754, 358518.99212383, 363958.72298911]])

As with the PointSkeleton, the GriddedSkeleton can also give its cartesian coordinates. However, since any UTM zone will be rotated in respect to the spherically defined structured grid, asking for the cartesian x-vector will cause a slight rotation. In other words, the same points can't be reguratly gridded in both shperical and UTM spaces :

.. code-block:: python

  >>> grid.utm.zone()
  (36, 'V')
  >>> grid.x()
  Regridding spherical grid to cartesian coordinates will cause a rotation! Use '_, y = skeleton.xy()' to get a list of all points.
  array([334729.73035137, 340236.28717586, 345743.09356756, 351250.14088716,
         356757.42049886, 362264.92377027])

To get the **exact** UTM coordinates of ALL the points, one can simply use:

.. code-block:: python

  x, y = grid.xy()


Underlying xarray Dataset structure
--------------------------------------------

As with the PointSkeleton, the structure is in an xarray Dataset (but longitude and latitue vectors are now coordinates, not variables):

.. code-block:: python
  >>> grid
  <GriddedSkeleton (Skeleton)>
  ------------------------------ Coordinate groups -------------------------------
  Spatial:    (lat, lon)
  Grid:       (lat, lon)
  Gridpoint:  *empty*
  All:        (lat, lon)
  ------------------------------------ Xarray ------------------------------------
  <xarray.Dataset> Size: 120B
  Dimensions:  (lat: 9, lon: 6)
  Coordinates:
    * lat      (lat) float64 72B 60.0 60.1 60.2 60.3 60.4 60.5 60.6 60.7 60.8
    * lon      (lon) float64 48B 30.0 30.1 30.2 30.3 30.4 30.5
  Data variables:
      *empty*
  Attributes:
      name:     LonelySkeleton
  --------------------------------------------------------------------------------

  >>> grid.ds()
  <xarray.Dataset> Size: 120B
  Dimensions:  (lat: 9, lon: 6)
  Coordinates:
    * lat      (lat) float64 72B 60.0 60.1 60.2 60.3 60.4 60.5 60.6 60.7 60.8
    * lon      (lon) float64 48B 30.0 30.1 30.2 30.3 30.4 30.5
  Data variables:
      *empty*
  Attributes:
      name:     LonelySkeleton

The size of the x- and y-vectors are given by:

.. code-block:: python

  >>> grid.nx()
  6
  >>> grid.ny()
  9

The size of the skeleton, defined by the lon-lat vecotrs, is given by:

.. code-block:: python

  >>> grid.size()
  (9, 6)


As an example, a cartesian PointSkeleton *could* be created from the spherical GriddedSkeleton as:

.. code-block:: python

  x, y = grid.xy()
  points = PointSkeleton(x=x, y=y)
  points.set_utm(grid.utm())


This now creates a new structure:

.. code-block:: python

  >>> points.ds()
  <xarray.Dataset>
  Dimensions:  (inds: 54)
  Coordinates:
    * inds     (inds) int64 0 1 2 3 4 5 6 7 8 9 ... 44 45 46 47 48 49 50 51 52 53
  Data variables:
      y        (inds) float64 6.655e+06 6.655e+06 ... 6.743e+06 6.743e+06
      x        (inds) float64 3.327e+05 3.383e+05 3.439e+05 ... 3.585e+05 3.64e+05
  Attributes:
      utm_zone:  36V

Nonetheless, converting between different types of skeletons is usually not needed, since a list of all the points in UTM-coordinates can be extracted directly from the spherical GriddedSkeleton. In other words, the two following lines give the exact same result:

.. code-block:: python

  x, y = grid.xy()
  x, y = points.xy()

The core of the Skeleton will still keep track of lon/lat as coordinates (to differentiate from actual data variables that can be added)

.. code-block:: python

  >>> grid.core.data_vars()
  []
  >>> grid.core.coords()
  ['lat', 'lon']
  >>> grid.core.coords('init') # A list of coordinates that are needed to initialize this class
  ['lat', 'lon']

Finding points
=============================================

Skeleton classes are equipped with a dedicated method to find points:

.. code-block:: python
   
   >>> data = GriddedSkeleton(lon=(3,5), lat=(60,61))
   >>> data.set_spacing(dm=1000)
   >>> grid.yank_point(lon=2.98, lat=60.01)
   {'inds_x': array([0]), 'inds_y': array([1]), 'dx': array([1120.6812202])}

   >>> points = PointSkeleton(lon=(3.0, 4.0, 5.0), lat=(60.0, 60.0, 61.0))
   >>> points.yank_point(lon=2.98, lat=60.01)
   {'inds': array([0]), 'dx': array([1576.18628188])}


This gives the corresponding index and distance to nearest point (in metres). The method can also be used to find several points:

.. code-block:: python
   
   >>> points.yank_point(lon=(2.98, 4.1), lat=(60.01, 60.01))
   {'inds': array([0, 1]), 'dx': array([1576.18628188, 5687.27546285])}

We can also find several closest points to one point:

.. code-block:: python
   
   >>> grid.yank_point(lon=2.98, lat=60.01, npoints=4)
   {'inds_x': array([0, 0, 0, 1]), 'inds_y': array([1, 2, 0, 1]), 'dx': array([1120.6812202 , 1428.55452856, 1576.18628188, 2131.94091801])}

If we know that a PointSkeleton structure has been raveled from a gridded structure, we can also find the gridded indeces after the fact:

.. code-block:: python
   
   >>> lon, lat = grid.lonlat()
   >>> raveled_grid = PointSkeleton(lon=lon, lat=lat)
   >>> raveled_grid.yank_point(lon=2.98, lat=60.01, npoints=4)
   {'inds': array([111, 222,   0, 112]), 'dx': array([1120.6812202 , 1428.55452856, 1576.18628188, 2131.94091801])}

   >>> raveled_grid.yank_point(lon=2.98, lat=60.01, npoints=4, gridded_shape=grid.size())
   {'inds': array([111, 222,   0, 112]), 'dx': array([1120.6812202 , 1428.55452856, 1576.18628188, 2131.94091801]), 'inds_x': array([0, 0, 0, 1]), 'inds_y': array([1, 2, 0, 1])}

Expanding **skeletons**
=============================================

Adding data variables
--------------------------------------------

The real benefit from skeletons is that you can define your own objects while still retaining all the original methods that are defined to handle the spatial coordinates. As an example, lets define an object that contains gridded significant wave height (hs) data:

.. code-block:: python

  from geo_skeletons import GriddedSkeleton
  from geo_skeletons.decorators import add_datavar

  @add_datavar(name='hs', default_value=0.)
  class WaveHeight(GriddedSkeleton):
    pass

The core of the Skeleton now keeps track of the data variable even though no data has been set.

.. code-block:: python
  >>> WaveHeight.core
  ------------------------------ Coordinate groups -------------------------------
  Spatial:    (y, x)
  Grid:       (y, x)
  Gridpoint:  *empty*
  All:        (y, x)
  ------------------------------------- Data -------------------------------------
  Variables:
      hs  (y, x):  0.0
  Masks:
      *empty*
  Magnitudes:
      *empty*
  Directions:
      *empty*
  --------------------------------------------------------------------------------

A better way is to use the geo-parameters package to add data variables:

.. code-block:: python

  from geo_skeletons import GriddedSkeleton
  from geo_skeletons.decorators import add_datavar
  import geo_parameters as gp

  @add_datavar(name=gp.wave.Hs('hs'), default_value=0.)
  class WaveHeight(GriddedSkeleton):
    pass

.. code-block:: python

  >>> WaveHeight.core
  ------------------------------ Coordinate groups -------------------------------
  Spatial:    (y, x)
  Grid:       (y, x)
  Gridpoint:  *empty*
  All:        (y, x)
  ------------------------------------- Data -------------------------------------
  Variables:
      hs  (y, x):  0.0 [m] sea_surface_wave_significant_height
  Masks:
      *empty*
  Magnitudes:
      *empty*
  Directions:
      *empty*
  --------------------------------------------------------------------------------

The ``@add_datavar`` decorator does the following:
  * adds a data variable names ``hs``
  * sets the default value for this variable to 0.0
  * creates a ``.hs()`` method to access that variable
  * creates a method ``.set_hs()`` that takes a numpy array

Using this new objects is now much like using the GriddedSkeleton, but the xarray Dataset now contains a data variable.

.. code-block:: python

  data = WaveHeight(lon=(3,5), lat=(60,61))
  data.set_spacing(dm=1000)

  >>> data.dx()
  996.8080414102963
  >>> data.dy()
  1001.4167247779651
  >>> data.hs() # Gives the default value
  array([[0., 0., 0., ..., 0., 0., 0.],
         [0., 0., 0., ..., 0., 0., 0.],
         [0., 0., 0., ..., 0., 0., 0.],
         ...,
         [0., 0., 0., ..., 0., 0., 0.],
         [0., 0., 0., ..., 0., 0., 0.],

  >>> data.hs(strict=True) # Returns 'None' if data has not been set
  None

This new data variable is contained in the underlying xarray Dataset

.. code-block:: python

  >>> data.ds() # hs-not saved since it is not set
  <xarray.Dataset> Size: 2kB
  Dimensions:  (lat: 112, lon: 111)
  Coordinates:
    * lat      (lat) float64 896B 60.0 60.01 60.02 60.03 ... 60.98 60.99 61.0
    * lon      (lon) float64 888B 3.0 3.018 3.036 3.055 ... 4.945 4.964 4.982 5.0
  Data variables:
      *empty*
  Attributes:
      name:     LonelySkeleton

.. code-block:: python

  >>> data.set_hs(5.6)
  >>> data.ds()
  <xarray.Dataset> Size: 101kB
  Dimensions:  (lat: 112, lon: 111)
  Coordinates:
    * lat      (lat) float64 896B 60.0 60.01 60.02 60.03 ... 60.98 60.99 61.0
    * lon      (lon) float64 888B 3.0 3.018 3.036 3.055 ... 4.945 4.964 4.982 5.0
  Data variables:
      hs       (lat, lon) float64 99kB 5.6 5.6 5.6 5.6 5.6 ... 5.6 5.6 5.6 5.6 5.6
  Attributes:
      name:     LonelySkeleton

The newly created ``.hs()`` method works directly with the xarray Dataset, and same slicing etc. possibilities work out of the box

.. code-block:: python

  >>> data.hs(lon=slice(4,4.5))
  array([[5.6, 5.6, 5.6, ..., 5.6, 5.6, 5.6],
         [5.6, 5.6, 5.6, ..., 5.6, 5.6, 5.6],
         [5.6, 5.6, 5.6, ..., 5.6, 5.6, 5.6],
         ...,
         [5.6, 5.6, 5.6, ..., 5.6, 5.6, 5.6],
         [5.6, 5.6, 5.6, ..., 5.6, 5.6, 5.6],
         [5.6, 5.6, 5.6, ..., 5.6, 5.6, 5.6]])


  >>> data.hs(lon=3)
  array([5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6])

  # Return xr.DataArray
  # Not that metadata is added when using a geo-parameter to create the class
  >>> data.hs(lon=2.98, method='nearest', data_array=True)
  <xarray.DataArray 'hs' (lat: 112)> Size: 896B
  array([5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6,
         5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6, 5.6])
  Coordinates:
    * lat      (lat) float64 896B 60.0 60.01 60.02 60.03 ... 60.98 60.99 61.0
      lon      float64 8B 3.0
  Attributes:
      short_name:     hs
      long_name:      significant_wave_height
      standard_name:  sea_surface_wave_significant_height
      units:          m

Adding additional coordinates
--------------------------------------------

Although all skeletons will have the x-y or lon-lat spatial coordinates, decorators can be used to add additional coordinates that the possible data is defined on. As an example, lets create a structure to represent wind data on a spherical 3D grid:

.. code-block:: python

   from geo_skeletons import GriddedSkeleton
   from geo_skeletons.decorators import add_coord, add_datavar

   @add_datavar(gp.wind.Wind, default_value=10.0)
   @add_coord(name="z", grid_coord=True)
   class WindSpeed(GriddedSkeleton):
       pass

.. code-block:: python

   >>> WindSpeed.core
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (y, x)
   Grid:       (y, x, z)
   Gridpoint:  *empty*
   All:        (y, x, z)
   ------------------------------------- Data -------------------------------------
   Variables:
       ff  (y, x, z):  10.0 [m/s] wind_speed
   Masks:
       *empty*
   Magnitudes:
       *empty*
   Directions:
       *empty*
   --------------------------------------------------------------------------------

The ``@add_coord`` decorator does the following:
  * adds a coordinate named ``z`` to signify the height
  * adds the requirement to provide the variable ``z`` when initializing the skeleton
  * adds a method ``.z()`` to access the values of this coordinate
  * adds a method ``.set_z_spacing()`` to set the spacing of the coordinate (only ``dx`` and ``nx`` keywords possible)

.. code-block:: python

   import numpy as np

   grid = WindSpeed(lon=(25, 30), lat=(58, 62), z=(0, 100))
   grid.set_spacing(dnmi=1)
   grid.set_z_spacing(dx=1)

   new_data = np.random.rand(grid.ny(), grid.nx(), len(grid.z()))
   grid.set_ff(new_data)

.. code-block:: python

   >>> grid
   <WindSpeed (GriddedSkeleton)>
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (lat, lon)
   Grid:       (lat, lon, z)
   Gridpoint:  *empty*
   All:        (lat, lon, z)
   ------------------------------------ Xarray ------------------------------------
   <xarray.Dataset> Size: 29MB
   Dimensions:  (lat: 241, lon: 151, z: 101)
   Coordinates:
     * lat      (lat) float64 2kB 58.0 58.02 58.03 58.05 ... 61.95 61.97 61.98 62.0
     * lon      (lon) float64 1kB 25.0 25.03 25.07 25.1 ... 29.9 29.93 29.97 30.0
     * z        (z) float64 808B 0.0 1.0 2.0 3.0 4.0 ... 96.0 97.0 98.0 99.0 100.0
   Data variables:
       ff       (lat, lon, z) float64 29MB 0.8337 0.1296 0.6254 ... 0.2336 0.1013
   --------------------------------------------------------------------------------

If we want to have several variables that are defined on different coordinate, we have some flixibility by using coordinate groups. If we, e.g. want to add a data variable that defines the surface rougness we can let the z-coordinate default to beeing added as a ``gridpoint`` coordinate instead of ``grid`` coordinate. We can then define over which coordinate group the data variable is added:

.. code-block:: python

   @add_datavar("roughness", default_value=0.0, coord_group="grid")
   @add_datavar(gp.wind.Wind, default_value=10.0, coord_group="all")
   @add_coord(name="z", grid_coord=False)
   class WindSpeed(GriddedSkeleton):
       pass

.. code-block:: python

   >>> WindSpeed.core
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (y, x)
   Grid:       (y, x)
   Gridpoint:  (z)
   All:        (y, x, z)
   ------------------------------------- Data -------------------------------------
   Variables:
       ff         (y, x, z):  10.0 [m/s] wind_speed
       roughness  (y, x):  0.0
   Masks:
       *empty*
   Magnitudes:
       *empty*
   Directions:
       *empty*
   --------------------------------------------------------------------------------


Adding a time, frequency and direction variable
--------------------------------------------

The adding of certain coordinates have dedicated method. As an example, let us use a wave spectrum:

.. code-block:: python

   from geo_skeletons import GriddedSkeleton
   from geo_skeletons.decorators import add_datavar, add_time, add_frequency, add_direction
   import geo_parameters as gp

   @add_datavar(gp.wave.Efth("spec"), coord_group="all")
   @add_direction()
   @add_frequency()
   @add_time()
   class Spectrum(GriddedSkeleton):
     pass

.. code-block:: python

   Spectrum.core
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (y, x)
   Grid:       (time, y, x)
   Gridpoint:  (freq, dirs)
   All:        (time, y, x, freq, dirs)
   ------------------------------------- Data -------------------------------------
   Variables:
       spec  (time, y, x, freq, dirs):  0.0 [m**2*s/rad] sea_surface_wave_directional_variance_spectral_density
   Masks:
       *empty*
   Magnitudes:
       *empty*
   Directions:
       *empty*
   --------------------------------------------------------------------------------

We can see that by default time is added as a grid coordinate, while frequency and direction are added as gridpoint coordinates. This doesn't affect the spectrum directly, since it is still defined over all coordinates. However, we can think of grid coordinates as "outer" coordinates, defining the grid on which we have a certain object that is defined over "inner" (gridpoint) coordinates. 

.. code-block:: python

   data = Spectrum(
      lon=(10, 20),
      lat=(50, 60),
      freq=np.arange(0, 1, 0.1),
      dirs=np.arange(0, 360, 10),
      time=("2020-01-01 00:00", "2020-01-02 00:00", "6h"),
   )

The add_frequency and add_direction creates method that can give out angular frequencies or the directions in radians

.. code-block:: python

   >>> data.freq()
   array([0. , 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
   >>> data.freq(angular=True)
   array([0.        , 0.62831853, 1.25663706, 1.88495559, 2.51327412,
          3.14159265, 3.76991118, 4.39822972, 5.02654825, 5.65486678])
   
   >>> data.dirs()
   array([  0,  10,  20,  30,  40,  50,  60,  70,  80,  90, 100, 110, 120,
          130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250,
          260, 270, 280, 290, 300, 310, 320, 330, 340, 350])
   >>> data.dirs(angular=True)
   array([0.        , 0.17453293, 0.34906585, 0.52359878, 0.6981317 ,
          0.87266463, 1.04719755, 1.22173048, 1.3962634 , 1.57079633,
          1.74532925, 1.91986218, 2.0943951 , 2.26892803, 2.44346095,
          2.61799388, 2.7925268 , 2.96705973, 3.14159265, 3.31612558,
          3.4906585 , 3.66519143, 3.83972435, 4.01425728, 4.1887902 ,
          4.36332313, 4.53785606, 4.71238898, 4.88692191, 5.06145483,
          5.23598776, 5.41052068, 5.58505361, 5.75958653, 5.93411946,
          6.10865238])

Useing the add_time decorator adds some time-specific functionality to the Skeleton:

.. code-block:: python

   >>> data.time()
   DatetimeIndex(['2020-01-01 00:00:00', '2020-01-01 06:00:00',
                  '2020-01-01 12:00:00', '2020-01-01 18:00:00',
                  '2020-01-02 00:00:00'],
                 dtype='datetime64[ns]', freq=None)

   >>> data.time(datetime=False)
   ['2020-01-01 00:00:00', '2020-01-01 06:00:00', '2020-01-01 12:00:00', '2020-01-01 18:00:00', '2020-01-02 00:00:00']
   
   >>> data.time(datetime=False, fmt='%Y%m%d')
   ['20200101', '20200101', '20200101', '20200101', '20200102']

To get only one time stamp for each day (or year or month or hour):

.. code-block:: python

   >>> data.days() # or .years(), .months(), .hours()
   DatetimeIndex(['2020-01-01', '2020-01-02'], dtype='datetime64[ns]', freq=None)
   
   >>> data.days(datetime=False)
   ['2020-01-01', '2020-01-02']
   
   >>> data.days(datetime=False, fmt='%Y%m%d')
   ['20200101', '20200102']


We can now also get the size of a single spectral object by looking at the size defined by the ``gridpoint`` coordinates

.. code-block:: python

   >>> data.size('gridpoint')
   (10, 36)

The coordinate groups also determines the order of iteration (iteration this way is easy, but slow for larger data sets):

.. code-block:: python

   for one_spec in data:
      pass

.. code-block:: python

   >>> one_spec
   <Spectrum (GriddedSkeleton)>
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (lat, lon)
   Grid:       (time, lat, lon)
   Gridpoint:  (freq, dirs)
   All:        (time, lat, lon, freq, dirs)
   ------------------------------------ Xarray ------------------------------------
   <xarray.Dataset> Size: 392B
   Dimensions:  (time: 1, lat: 1, lon: 1, freq: 10, dirs: 36)
   Coordinates:
     * time     (time) datetime64[ns] 8B 2020-01-02
     * lat      (lat) int64 8B 60
     * lon      (lon) int64 8B 20
     * freq     (freq) float64 80B 0.0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
     * dirs     (dirs) int64 288B 0 10 20 30 40 50 60 ... 300 310 320 330 340 350
   Data variables:
       *empty*
   Attributes:
       name:     LonelySkeleton
   ---------------------------------- Empty data ----------------------------------
   Empty variables:
       spec  (time, lat, lon, freq, dirs):  0.0 [m**2*s/rad] sea_surface_wave_directional_variance_spectral_density
   --------------------------------------------------------------------------------

   >>> one_spec.spec().shape # The method automatically squeezes out trivial dimensions
   (10, 36)

   >>> spec.spec(squeeze=False).shape
   (1, 1, 1, 10, 36)

   >>> one_spec.set_spec(one_spec.spec()*2) # Automatically expands trivial dimensions



Setting data
--------------------------------------------

The Skeletons ``set``-method is helpful if the data at hand does not conform exactly to the format specified in the Skeleton. First, the Skeleton can squeeze and unsqueeze trivial dimensions at will. Let's define a Skeleton for significant wave height from a wave forecast:


.. code-block:: python

   from geo_skeletons import GriddedSkeleton
   from geo_skeletons.decorators import add_datavar, add_time
   import geo_parameters as gp

   @add_datavar(gp.wave.Hs)
   @add_time()
   class WaveForecast(GriddedSkeleton):
       pass

   data = WaveForecast(
       lon=(0, 10),
       lat=(50, 60),
       time=("2020-01-01 00:00", "2020-01-03 00:00", "3h"),
   )
   data.set_spacing(nx=11, ny=21)

The ``hs``-variable now has a given shape:

.. code-block:: python

   >>> data.shape('hs')
   (17, 21, 11) # time, lat, lon


Let us say we get some data that is taken from an ensemble forecast, but cut to only the deterministic member, but that dimension has not been squeezed out:

.. code-block:: python

   >>> eps_hs.shape
   (17, 1, 21, 11)

The Skeleton ``set``-method can automatically get rid of this extra trivial dimension:
   
.. code-block:: python

   data.set_hs(eps_hs) # Or data.set('hs', eps_hs)

This could have been done by ``np.squeeze``, but doing that can cause troubles if we have other trivial dimensions, such as only one time.

The Skeleton can also re-arange non-trivial dimensions. Say that we get data from another forecast that has dimensions ``['lon','lat','ensembe','time']`` (Skeleton has ``['time','lat','lon']``). We can then specify the order in the set method:

.. code-block:: python
   
   >>> eps2_hs.shape
   (11, 21, 1, 17)

   >>> data.set_hs(eps2_hs)
   *** geo_skeletons.errors.DataWrongDimensionError: Data has shape (11, 21, 1, 17), but coordinates define a shape (17, 21, 11)!!!

   >>> data.set_hs(eps2_hs, coords=['lon', 'lat', 'time']) # Not that we only need to specify order of non-trivial dimensions
   >>> data.shape('hs')
   (17, 21, 11)

If we give in an ``xr.DataArray`` instead of a ``np.array``, then the order of the coordinates wil be parsed from the metadata in the ``DataArray`` if possible, but this can always be overridden by using the ``coords`` keyword.


If we only have two dimensions, then we can allow the Skeleton to try a transpose of the data:

.. code-block:: python

   @add_datavar(gp.wave.Hs)
   class WaveData(GriddedSkeleton):
       pass
   
   grid = WaveData(lon=(10, 20), lat=(50, 60))
   grid.set_spacing(nx=10, ny=20)

.. code-block:: python

   data = np.ones((10, 20))
   
   >>> grid.set_hs(data)
   *** geo_skeletons.errors.DataWrongDimensionError: Data has shape (10, 20), but coordinates define a shape (20, 10)!!!

   >>> grid.set_hs(data, allow_transpose=True)
   >>> np.mean(grid.hs())
   np.float64(1.0)

As previously, trivial dimensions are squeezed out prior to the transpose:

.. code-block:: python

   >>> data = np.full((1,10,1,20,1), 5.5)

   >> grid.set_hs(data)
   *** geo_skeletons.errors.DataWrongDimensionError: Data has shape (1, 10, 1, 20, 1), but coordinates define a shape (20, 10)!!!
   
   >>> grid.set_hs(data, allow_transpose=True)
   >>> np.mean(grid.hs())
   np.float64(5.5)

Adding magnitudes and directions
--------------------------------------------

The recommended way to add magnitudes and directions are to add the components as data variables and add a magnitude and direction connected to those components:

.. code-block:: python

   from geo_skeletons import GriddedSkeleton
   from geo_skeletons.decorators import add_datavar, add_magnitude

   import geo_parameters as gp

   @add_magnitude(gp.wind.Wind("u"), x="ux", y="uy", direction=gp.wind.WindDir("ud"))
   @add_datavar(gp.wind.YWind("uy"))
   @add_datavar(gp.wind.XWind("ux"))
   class Wind(GriddedSkeleton):
       pass

.. code-block:: python

   >>> Wind.core
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (y, x)
   Grid:       (y, x)
   Gridpoint:  *empty*
   All:        (y, x)
   ------------------------------------- Data -------------------------------------
   Variables:
       ux  (y, x):  0.1 [m/s] x_wind
       uy  (y, x):  0.1 [m/s] y_wind
   Masks:
       *empty*
   Magnitudes:
     u: magnitude of (ux,uy) [m/s] wind_speed
   Directions:
     ud: direction of (ux,uy) [deg] wind_from_direction
   --------------------------------------------------------------------------------

The ``@add_magnitude`` decorator does the following:
  * adds a magnitude ``u`` for the components ``ux`` and ``uy``
  * adds a direction (optional) ``ud`` for the components ``ux`` and ``uy``
  * parses the standard_name of gp.wind.WindDir to identify that this is a 'from' direction
  * creates ``.u()`` and ``.ud()`` methods to access the magnitude and direction
  * creates ``.set_u()`` and ``.set_ud()`` methods to set the magnitude and direction

Now we can still set the wind speed and direction, but only the components will be stored in the Dataset (obviously the components can also still be set and accessed normally):

.. code-block:: python

   >>> data = Wind(lon=(10, 14), lat=(50, 60))
   >>> data.set_u(10)
   >>> data.set_ud(45)

   >>> data
   <Wind (GriddedSkeleton)>
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (lat, lon)
   Grid:       (lat, lon)
   Gridpoint:  *empty*
   All:        (lat, lon)
   ------------------------------------ Xarray ------------------------------------
   <xarray.Dataset> Size: 96B
   Dimensions:  (lat: 2, lon: 2)
   Coordinates:
     * lat      (lat) int64 16B 50 60
     * lon      (lon) int64 16B 10 14
   Data variables:
       ux       (lat, lon) float64 32B -7.071 -7.071 -7.071 -7.071
       uy       (lat, lon) float64 32B -7.071 -7.071 -7.071 -7.071
   Attributes:
       name:     LonelySkeleton
   -------------------------- Magnitudes and directions ---------------------------
     u: magnitude of (ux,uy) [m/s] wind_speed
     ud: direction of (ux,uy) [deg] wind_from_direction
   --------------------------------------------------------------------------------

The directionality of the wind was parsed from the standard name of the geo-parameter gp.wind.WindDir. Therefore the components could be set properly and unambiguously. The direction can be set and retrieved in different directional conventions:

.. code-block:: python

   >>> data.set_ud(270, dir_type='to')
   >>> data.ud(dir_type='from')
   array([[90., 90.],
          [90., 90.]])
   >>> data.ud(dir_type='to')
   array([[270., 270.],
          [270., 270.]])
   >>> data.ud(dir_type='math')
   array([[3.14159265, 3.14159265],
          [3.14159265, 3.14159265]])

This same functionality is also present for individual parameters of directional nature (that typically don't use components):

.. code-block:: python

   # This is direction from, as determined by the standard name
   # To get direction to, use gp.wave.DirpTo
   @add_datavar(gp.wave.Dirp) 
   class Wave(GriddedSkeleton):
      pass

.. code-block:: python

   >>> data.core
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (lat, lon)
   Grid:       (lat, lon)
   Gridpoint:  *empty*
   All:        (lat, lon)
   ------------------------------------- Data -------------------------------------
   Variables:
       dirp  (lat, lon):  0.0 [deg] sea_surface_wave_from_direction_at_variance_spectral_density_maximum
   Masks:
       *empty*
   Magnitudes:
       *empty*
   Directions:
       *empty*
   --------------------------------------------------------------------------------

.. code-block:: python

   data = Wave(lon=(10, 14), lat=(50, 60))
   data.set_dirp(45, dir_type="to")
   
   >>> data.dirp() # default dir_type is the one that the parameter is, in this case 'from'
   array([[225., 225.],
          [225., 225.]])


Adding masks
--------------------------------------------

Logical masks (for example marking land points or points of interest) can be added to the skeletons. To for example add a land-sea mask to a gridded significant wave height:

.. code-block:: python

   from geo_skeleton.decorators import add_datavar, add_mask
   import geo_parameters as gp
   
   @add_datavar(gp.wave.Hs("hs"), default_value=0.0)
   @add_mask(
       name="sea",
       default_value=0,
       coord_group="grid",
       opposite_name="land",
   )
   class WaveHeight(GriddedSkeleton):
       pass

The geo-skeleton now has a land-sea mask. Only the sea mask is stored in the xr.Dataset and the land mask is calculated as the inverse of the sea mask. 

.. code-block:: python

   >>> grid = WaveHeight(lon=(0, 1), lat=(60, 61), name="Wavegrid")

   >>> grid
   <WaveHeight (GriddedSkeleton)>
   ------------------------------ Coordinate groups -------------------------------
   Spatial:    (lat, lon)
   Grid:       (lat, lon)
   Gridpoint:  *empty*
   All:        (lat, lon)
   ------------------------------------ Xarray ------------------------------------
   <xarray.Dataset> Size: 32B
   Dimensions:  (lat: 2, lon: 2)
   Coordinates:
   * lat      (lat) int64 16B 60 61
   * lon      (lon) int64 16B 0 1
   Data variables:
    *empty*
   Attributes:
    name:     Wavegrid
   ---------------------------------- Empty data ----------------------------------
   Empty variables:
    hs  (lat, lon):  0.0 [m] sea_surface_wave_significant_height
   Empty masks:
    sea_mask   (lat, lon):  False
    land_mask  (lat, lon):  True
   --------------------------------------------------------------------------------

Both are still gettable, and also the points corresponding to the masks have methods:

.. code-block:: python

   >>> grid.sea_mask()
   array([[False, False],
          [False, False]])
   >>> grid.land_mask()
   array([[ True,  True],
          [ True,  True]])
   
   >>> grid.sea_points()
   (array([], dtype=int64), array([], dtype=int64))
   >>> grid.land_points()
   (array([0, 1, 0, 1]), array([60, 60, 61, 61]))

We can also set either the land mask or the sea mask, and that defines boths masks, since they are per definition opposites of each other:

.. code-block:: python

   >>> grid.set_sea_mask([[True, False], [True, False]])
   >>> grid.sea_mask()
   array([[ True, False],
          [ True, False]])
   >>> grid.sea_points()
   (array([0, 0]), array([60, 61]))
   
   >>> grid.land_mask()
   array([[False,  True],
          [False,  True]])
   >>> grid.land_points()
   (array([1, 1]), array([60, 61]))


It is also possible to connect the masks to a certain data variable. E.g. to have only positive significant wave height values to be sea points:

.. code-block:: python

   @add_datavar(gp.wave.Hs("hs"), default_value=0.0)
   @add_mask(
       name="sea",
       default_value=0,
       coord_group="grid",
       opposite_name="land",
       triggered_by="hs",
       valid_range=(0, None),
       range_inclusive=False,
   )
   class WaveHeight(GriddedSkeleton):
       pass

Here ``triggered_by`` means that setting the data variable ``hs`` sets the land-sea masks. ``valid_range`` means the ``hs`` values that define the primary mask (here ``sea_mask``), and ``range_inclusive=False`` means that ``0`` does **not** become a sea point:

.. code-block:: python

   >>> grid.set_hs([[0,1],[2,-999]])
   >>> grid.hs()
   array([[   0,    1],
          [   2, -999]])
   
   >>> grid.sea_mask()
   array([[False,  True],
          [ True, False]])
   >>> grid.land_points()
   (array([0, 1]), array([60, 61]))

The masks are still settable independently if needed (e.g. if onw wants to apply a separate land mask after the fact), but if they are not set then they are always "in sync" with the significant wave height data.

Plotting the data
--------------------------------------------

Skeletons don't have any plotting functionality built in, but since it wraps around xarray datasets, the xarray plotting functions can be used. We can take as an example the wind class defined in the "Adding magnitudes and directions" section. Note, that even though the wind speed (added as a magnitude of the components) is not stored in the underlying xr.Dataset, we can still get it as a DataArray and plot it:

.. code-block:: python

   import numpy as np
   import matplotlib.pyplot as plt
   
   data = Wind(lon=(0, 10), lat=(50, 60))
   data.set_spacing(dm=1000)
   new_data = np.random.rand(*data.size())
   data.set_u(new_data)
   data.u(data_array=True).plot()
   plt.show()
  
.. image:: example_wind_plot.png

Here, the ``data_array=True`` tells the method to return the xarray data array instead of a numpy array of the values.
