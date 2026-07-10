Basic classes
=============================================
All the python classes you build using geo-skeletons inherit from one of two basic classes. They are not Abstract base classes, but normal classes that can be used to represent geographical points in any projection. They can then be extended to classes containen data for those points, or to have other coordinates (such as time).

Each of the basic classes can in turn be natively either on a spherical grid, a cartesian non-projected grid, a cartesian projected grid or a non-cartesian projected grid. But this is done upon initialization of an instance. In other words, also every extension of the basic classes will have full functionality in all projections.

PointSkeleton
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The PointSkeleton is meant to represent data points that have no gridded structure. To create an instance on normal longitudes and latitudes:

.. code-block:: python

  from geo_skeletons import PointSkeleton
  points = PointSkeleton(lon=(30.0,30.1,30.5), lat=(60.0,60.0,60.8))

The points are now accessible in both lon-lat, and projected x-y coordinates. The projection (CRS, coordinate reference system) is by default the best UTM projection, since no other projection was specified.

.. code-block:: python

  >>> points.lon()
  array([30. , 30.1, 30.5])

  >>> points.proj.crs()
  (36, 'V')
  
  >>> points.x()
  array([332705.17887694, 338279.24910909, 363958.72298911])

The values can also be requestin in some other coordinate system without changing the set CRS:

.. code-block:: python

  >>> points.x(crs=(33,'W')) # Another UTM zone
  array([1331808.13859715, 1337286.99102854, 1338117.44887216])
  
  >>> points.x(crs=5951) # Using an EPSG code
  array([1123104.5678867 , 1128532.07283879, 1124321.26448899])

For more on details on the CRS's, see the section on projections.

GriddedSkeleton
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

The GriddedSkeleton is meant to represent data points that are defined on a grid by an x- and y-vector (or lon- ant lat-vector)

.. code-block:: python

    from geo_skeletons import GriddedSkeleton
    grid = GriddedSkeleton(lon=(30.0,30.5), lat=(60.0,60.8))

Now the instance is only defined on four points (the corners). To make a grid, set a spacing for the instance:

.. code-block:: python

    >>> grid.lon()
    array([30. , 30.5])

    >>> grid.set_spacing(dlon=0.1, dlat=0.1)
    
    >>> grid.lon()
    array([30. , 30.1, 30.2, 30.3, 30.4, 30.5])
    
    >>> grid.lat()
    array([60. , 60.1, 60.2, 60.3, 60.4, 60.5, 60.6, 60.7, 60.8])

Meaning of being "natively" spherical/projected
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
