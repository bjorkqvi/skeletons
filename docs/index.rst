Welcome to skeletons's documentation!
=====================================

**skeletons** is a Python package that provides a structure for gridded and ungridded data. It supports both spherical (lon-lat) and cartesian (UTM) coordinates. A skeleton is initialized as either spherical or cartesian, but it can automatically convert its native coordinates.

This skeleton structure can easily be expanded to data structures if decorators are used to add additional spatial or temporal dimensons or data variables.



Using PointSkeletons
=============================================

Creation
---------------------------------------------

A point cloud can easily be represented as an unstructured skeleton:

.. code-block:: python

  from geo_skeletons.point_skeleton import PointSkeleton
  points = PointSkeleton(lon=(30.0,30.1,30.5), lat=(60.0,60.0,60.8))


Accessing coordinates
---------------------------------------------

Coordinates are now accessible both in the (native) spherical and the (non-native) cartesian versions:

.. code-block:: python

  >>> points.lon()
  array([30. , 30.1, 30.5])
  >>> points.x()
  array([332705.17887694, 338279.24910909, 363958.72298911])


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


Cartesian UTM coordinates
---------------------------------------------

The UTM zone that is used was automatically set to the one most compatible with the spherical coordinates:

.. code-block:: python

  >>> points.utm()
  (36, 'V')


This can be changed and resetted as follows:

.. code-block:: python

  >>> points.set_utm((35,'V'))
  Setting UTM (35, V)
  >>> points.x()
  array([667294.82112306, 672868.6361206 , 690427.36544455])
  >>> points.set_utm()
  Setting UTM (36, V)
  >>> points.x()
  array([332705.17887694, 338279.24910909, 363958.72298911])


Underlying xarray Dataset structure
--------------------------------------------

The skeleton information is stored in an xarray Dataset. This will be convenient when the skeleton is expanded by additional coordinates or variables:

.. code-block:: python

  >>> points.ds()
  <xarray.Dataset>
  Dimensions:  (inds: 3)
  Coordinates:
    * inds     (inds) int64 0 1 2
  Data variables:
      lat      (inds) float64 60.0 60.0 60.8
      lon      (inds) float64 30.0 30.1 30.5

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


Using GriddedSkeletons
=============================================

Creation and setting spacing
---------------------------------------------

Unlike a PointSkeleton, a GriddedSkeleton is defined on an area:

.. code-block:: python

  from geo_skeletons.gridded_skeleton import GriddedSkeleton
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


Cartesian UTM coordinates
---------------------------------------------

As with the PointSkeleton, the GriddedSkeleton can also give its cartesian coordinates. However, since any UTM zone will be rotated in respect to the spherically defined structured grid, asking for the cartesian x-vector will cause a slight rotation. In other words, the same points can't be reguratly gridded in both shperical and UTM spaces :

.. code-block:: python

  >>> grid.utm()
  (36, 'V')
  >>> grid.x()
  Regridding spherical grid to cartesian coordinates. This will cause a rotation!
  array([334729.73035137, 340236.28717586, 345743.09356756, 351250.14088716,
         356757.42049886, 362264.92377027])

To get the **exact** UTM coordinates of ALL the points, one can simply use:

.. code-block:: python

  x, y = grid.xy()


Underlying xarray Dataset structure
--------------------------------------------

As with the PointSkeleton, the structure is in an xarray Dataset (but longitude and latitue vectors are now coordinates, not variables):

.. code-block:: python

  >>> grid.ds()
  <xarray.Dataset>
  Dimensions:  (lat: 9, lon: 6)
  Coordinates:
    * lat      (lat) float64 60.0 60.1 60.2 60.3 60.4 60.5 60.6 60.7 60.8
    * lon      (lon) float64 30.0 30.1 30.2 30.3 30.4 30.5
  Data variables:
      *empty*

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


Expanding **skeletons**
=============================================

Adding data variables
--------------------------------------------

The real benefit from skeletons is that you can define your own objects while still retaining all the original methods that are defined to handle the spatial coordinates. As an example, lets define an object that contains gridded significant wave height (hs) data:

.. code-block:: python

  from geo_skeletons.gridded_skeleton import GriddedSkeleton
  from geo_skeletons.decorators import add_datavar

  @add_datavar(name='hs', default_value=0.)
  class WaveHeight(GriddedSkeleton):
    pass


Using this new objects is now much like using the GriddedSkeleton, but the xarray Dataset now contains a data variable, and the skeleton automatically creates an ``.hs()`` method to access the wave height data.

.. code-block:: python

  data = WaveHeight(lon=(3,5), lat=(60,61))
  data.set_spacing(dm=1000)

  >>> data.dx()
  1000.9278895090348
  >>> data.dy()
  1004.1755527666762
  >>> data.hs()
  array([[0., 0., 0., ..., 0., 0., 0.],
         [0., 0., 0., ..., 0., 0., 0.],
         [0., 0., 0., ..., 0., 0., 0.],
         ...,
         [0., 0., 0., ..., 0., 0., 0.],
         [0., 0., 0., ..., 0., 0., 0.],
   
This new data variable is contained in the underlying xarray Dataset

.. code-block:: python

  >>> data.ds()
  <xarray.Dataset>
  Dimensions:  (lat: 120, lon: 119)
  Coordinates:
    * lat      (lat) float64 60.0 60.01 60.02 60.03 ... 60.97 60.98 60.99 61.0
    * lon      (lon) float64 3.0 3.017 3.034 3.051 3.068 ... 4.949 4.966 4.983 5.0
  Data variables:
      hs       (lat, lon) float64 0.0 0.0 0.0 0.0 0.0 0.0 ... 0.0 0.0 0.0 0.0 0.0

The newly created ``.hs()`` method works directly with the xarray Dataset, and same slicing etc. possibilities work out of the box

.. code-block:: python

  >>> data.hs(lon=slice(4,4.5))
  array([[0., 0., 0., ..., 0., 0., 0.],
       [0., 0., 0., ..., 0., 0., 0.],
       [0., 0., 0., ..., 0., 0., 0.],
       ...,
       [0., 0., 0., ..., 0., 0., 0.],
       [0., 0., 0., ..., 0., 0., 0.],
       [0., 0., 0., ..., 0., 0., 0.]])

  >>> data.hs(lon=3)
  array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0.])

  >>> data.hs(lon=2.98, method='nearest')
  array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.,
         0.])

Finding points
--------------------------------------------

Although the above method of slicing is useful, it is not optimal when one want's to find a point nearest to a given coordinate. This is especially true for non-gridded data where one simply can't find the nearest longitude and latitude separately. Skeleton classes are therefore equipped with a dedicated method:

.. code-block:: python

  >>> point_dict = data.yank_point(lon=2.98, lat=60.01)
  {'inds_x': array([0]), 'inds_y': array([1]), 'dx': array([1129.78211206])}

This gives the corresponding index and distance to nearest point (in metres). The method takes tuples, and can be used also for gridded data (then the x- and y-index is returned seprately in the dict). 

The method can also be used to find the nearest longitude-latitude point in a grid that has been defined on cartesian UTM-coordinates (or vice versa).




Adding masks
--------------------------------------------

Logical masks (for example marking land points or points of interest) can be added to the skeletons. To for example add a land-sea mask to the previous example:

.. code-block:: python

  from geo_skeletons.gridded_skeleton import GriddedSkeleton
  from geo_skeletons.datavar_factory import add_datavar, add_mask

  @add_datavar(name='hs', default_value=0.)
  @add_mask(name='sea', default_value=1, coords='grid', opposite_name='land')
  class WaveHeight(GriddedSkeleton):
    pass

This creates a logical mask that has value true for sea points. The ``opposite_name`` options automatically create a land mask that is false for poits that are not sea points.

.. code-block:: python

  data = WaveHeight(lon=(3,5), lat=(60,61))
  data.set_spacing(dm=1000)

  >>> data.land_mask()
  array([[False, False, False, ..., False, False, False],
         [False, False, False, ..., False, False, False],
         [False, False, False, ..., False, False, False],
         ...,
         [False, False, False, ..., False, False, False],
         [False, False, False, ..., False, False, False],
         [False, False, False, ..., False, False, False]])
  
  >>> data.sea_mask()
  array([[ True,  True,  True, ...,  True,  True,  True],
         [ True,  True,  True, ...,  True,  True,  True],
         [ True,  True,  True, ...,  True,  True,  True],
         ...,
         [ True,  True,  True, ...,  True,  True,  True],
         [ True,  True,  True, ...,  True,  True,  True],
         [ True,  True,  True, ...,  True,  True,  True]])


To set a new mask, you can use the created methods. To e.g. set all points to land, you can either set sea points to false

.. code-block:: python

  new_mask = np.zeros(data.size())
  data.set_sea_mask(new_mask)

or land points to true

.. code-block:: python

  new_mask = np.ones(data.size())
  data.set_land_mask(new_mask)

Only the sea mask is saved in the underlying xarray dataset, meaning that the land and the sea mask will always be consistent.

.. code-block:: python

  >>> data.ds()
  <xarray.Dataset>
  Dimensions:   (lat: 120, lon: 119)
  Coordinates:
    * lat       (lat) float64 60.0 60.01 60.02 60.03 ... 60.97 60.98 60.99 61.0
    * lon       (lon) float64 3.0 3.017 3.034 3.051 ... 4.949 4.966 4.983 5.0
  Data variables:
      sea_mask  (lat, lon) int64 0 0 0 0 0 0 0 0 0 0 0 0 ... 0 0 0 0 0 0 0 0 0 0 0
      hs        (lat, lon) float64 0.0 0.0 0.0 0.0 0.0 0.0 ... 0.0 0.0 0.0 0.0 0.0

You can also access the coordinates of the points in the mask. Let's set all points to sea points except the northern (upper) edge:

.. code-block:: python

  new_mask = np.ones(data.size())
  new_mask[0,:] = 0
  data.set_sea_mask(new_mask)

  lon, lat = data.land_points()

Where ``lon`` and ``lat`` now gives the coordinates of the land points. Notice, that this is just a convinently named wrapper for

.. code-block:: python

  lon, lat = data.lonlat(mask=data.land_mask())

Adding additional coordinates
--------------------------------------------

Although all skeletons will have the x-y or lon-lat spatial coordinates, decorators can be used to add additional coordinates that the possible data is defined on. As an example, lets create a structure to keep water temperature data on a spherical 3D grid:

.. code-block:: python

  from geo_skeletons.gridded_skeleton import GriddedSkeleton
  from geo_skeletons.coordinate_factory import add_coord, add_datavar
  import numpy as np


  @add_datavar(name="water_temperature", default_value=10.0)
  @add_coord(name="z", grid_coord=True)
  class a3DGrid(GriddedSkeleton):
      pass


  grid = a3DGrid(lon=(25, 30), lat=(58, 62), z=(0, 100))
  grid.set_spacing(dnmi=1)
  grid.set_z_spacing(dx=1)

  new_data = np.random.rand(grid.ny(), grid.nx(), len(grid.z()))
  grid.set_water_temperature(new_data)


The ``@add_coord`` decorator does the following:
  * adds a coordinate named ``z`` to signify the depth
  * adds the possibility to provide the variable ``z`` when initializing the skeleton
  * adds a method ``.z()`` to access the values of this coordinate
  * adds a method ``.set_z_spacing()`` to set the spacing of the coordinate (only ``dx`` and ``nx`` keywords possible)
  
The ``@add_datavar`` decordator does the following:
  * adds a data variable names ``water_temperature`` defined on all three coordinates
  * sets the default value for this variable to 10
  * creates a ``.water_temperature()`` method to access that variable
  * creates a method ``.set_water_temperature()`` that takes a numpy array
  

Plotting the data
--------------------------------------------

Skeletons don't have any plotting functionality built in, but since it wraps around xarray datasets, the xarray plotting functions can be used. To plot the surface temperature in the previous example, just use:

.. code-block:: python

  import matplotlib.pyplot as plt
  
  grid.water_temperature(z=0, data_array=True).plot()
  plt.show()
  
.. image:: simple_plot.png

Here, the ``data_array=True`` tells the method to return the xarray data array instead of a numpy array of the values.
