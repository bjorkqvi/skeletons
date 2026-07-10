import numpy as np

def test_point():
    from geo_skeletons import PointSkeleton
    points = PointSkeleton(lon=(30.0,30.1,30.5), lat=(60.0,60.0,60.8))
    assert points.proj.crs() == (36, 'V')
    np.testing.assert_array_almost_equal(points.x(crs=(33,'W')) ,np.array([1331808.13859715, 1337286.99102854, 1338117.44887216]))
    np.testing.assert_array_almost_equal(points.x(crs=5951) ,np.array([1123104.5678867 , 1128532.07283879, 1124321.26448899]))


def test_gridded():
    from geo_skeletons import GriddedSkeleton
    grid = GriddedSkeleton(lon=(30.0,30.5), lat=(60.0,60.8))
    grid.set_spacing(dlon=0.1, dlat=0.1)
