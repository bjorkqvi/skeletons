from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np

def test_cut_point_spherical_spherical():
    points = PointSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50))
    grid = GriddedSkeleton(lon=(2,4), lat=(20,30))
    cut_points = points.cut_to_area(grid)
    np.testing.assert_array_almost_equal(cut_points.lon(), [2,3])
    np.testing.assert_array_almost_equal(cut_points.lat(), [20,30])

    points = GriddedSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50)).ravel()
    grid = GriddedSkeleton(lon=(2,4), lat=(20,30))
    cut_points = points.cut_to_area(grid)

    np.testing.assert_array_almost_equal(cut_points.lon(), [2,3,4,2,3,4])
    np.testing.assert_array_almost_equal(cut_points.lat(), [20,20,20,30,30,30])


def test_cut_gridded_spherical_spherical():
    points = GriddedSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50))
    grid = GriddedSkeleton(lon=(2,4), lat=(20,30))
    cut_points = points.cut_to_area(grid)
    np.testing.assert_array_almost_equal(cut_points.lon(), [2,3,4])
    np.testing.assert_array_almost_equal(cut_points.lat(), [20,30])


def test_cut_point_cartesian_cartesian():
    points = PointSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50))
    points = points.ravel(proj='xy')
    x,y = points.xy()

    grid = GriddedSkeleton(x=x[1:3], y=y[1:3], crs=points.proj.crs())
    cut_points = points.cut_to_area(grid)
    np.testing.assert_array_almost_equal(cut_points.x(), x[1:3])
    np.testing.assert_array_almost_equal(cut_points.y(), y[1:3])

def test_cut_gridded_cartesian_cartesian():
    points = GriddedSkeleton(x=(280766.86117078,643329.12388609), y=(1106077.12969475, 5540547.37030426),crs=(31, 'P'))
    points.set_spacing(nx=15,ny=15)
    x,y = points.x(), points.y()
    grid = GriddedSkeleton(x=(x[5],x[7]), y=(y[8],y[10]), crs=(31,'P'))
    cut_points = points.cut_to_area(grid)

    np.testing.assert_array_almost_equal(cut_points.x(), x[5:8])
    np.testing.assert_array_almost_equal(cut_points.y(), y[8:11])

def test_cut_point_spherical_cartesian():
    points = PointSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50))
    points = points.ravel(proj='xy')
    x,y = points.xy()

    grid = GriddedSkeleton(x=x[1:3], y=y[1:3], crs=points.proj.crs())
    points = PointSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50))
    cut_points = points.cut_to_area(grid)
    np.testing.assert_array_almost_equal(cut_points.x(), x[1:3],decimal=3)
    np.testing.assert_array_almost_equal(cut_points.y(), y[1:3], decimal=3)

def test_cut_point_cartesian_spherical():
    points = PointSkeleton(lon=(1,2,3,4,5), lat=(10,20,30,40,50))
    points = points.ravel(proj='xy')
    x,y = points.xy()

    grid = GriddedSkeleton(lon=(1.9,3.1), lat=(19.9,30.1))
    cut_points = points.cut_to_area(grid)
    np.testing.assert_array_almost_equal(cut_points.x(), x[1:3],decimal=3)
    np.testing.assert_array_almost_equal(cut_points.y(), y[1:3], decimal=3)


def test_cut_gridded_cartesian_sperical():
    points = GriddedSkeleton(x=(280766.86117078,643329.12388609), y=(1106077.12969475, 5540547.37030426),crs=(31, 'P'))
    points.set_spacing(nx=15,ny=15)

    grid = GriddedSkeleton(lon=(1.9,3.1), lat=(19.9,30.1))
    cut_points = points.cut_to_area(grid)
    lon, lat = cut_points.lonlat()
    assert lon.max()<=3.1
    assert lon.min()>=1.9
    assert lat.max()<=30.1
    assert lat.min()>=19.9


def test_cut_gridded_sperical_cartesian():
    points = GriddedSkeleton(lon=(2,3), lat=(20,30))
    points.set_spacing(nx=15,ny=15)

    grid = GriddedSkeleton(x=(280766.86117078,643329.12388609), y=(1106077.12969475, 5540547.37030426),crs=(31, 'P'))
    cut_points = points.cut_to_area(grid)
    x, y = cut_points.xy()
    assert x.max()<=643329.12388609
    assert x.min()>=280766.86117078
    assert y.max()<=5540547.37030426
    assert y.min()>=1106077.12969475


def test_cut_gridded_cartesian_cartesian_reproj():
    points = GriddedSkeleton(x=(280766.86117078,643329.12388609), y=(1106077.12969475, 5540547.37030426),crs=(31, 'P'))
    points.set_spacing(nx=15,ny=15)

    grid = GriddedSkeleton(x=(-880498.88310001,-650693.22581457), y=(2254799.98328021,3390344.36385531), crs=(33,'W'))

    cut_points = points.cut_to_area(grid)

    x, y = cut_points.xy(crs=(33,'W'))
    assert x.max()<=-650693.22581457
    assert x.min()>=-880498.88310001
    assert y.max()<=3390344.36385531
    assert y.min()>=2254799.98328021