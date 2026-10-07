from geo_skeletons import GriddedSkeleton, PointSkeleton
import geo_parameters as gp
import numpy as np

def test_unproject_point():
    Proj = PointSkeleton.add_datavar([gp.wind.XWind('u'), gp.wind.YWind('v')]).add_magnitude(gp.wind.Wind)

    points = Proj(lon=(10,11,12), lat=(50,51,52))
    x,y = points.xy()
    points = Proj(x=x, y=y, crs=points.proj.utm())
    points.set_ff(10)
    
    points.set_dd(180)
    points.dd()
    UnProj = Proj.unproject_class()
    
    points2 = UnProj.from_ds(points.ds())

    np.testing.assert_array_almost_equal(points.ff(), points2.ff())
    np.testing.assert_array_almost_equal(points.dd(), points2.dd())
    np.testing.assert_array_almost_equal(points.u(wrt='lonlat'), points2.u())
    np.testing.assert_array_almost_equal(points.v(wrt='lonlat'), points2.v())
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points.u(), 0)
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points.v(), 10)
    np.testing.assert_array_almost_equal(points2.u(),0)
    np.testing.assert_array_almost_equal(points2.v(),10)

    assert points.core.get('u').meta.is_same(gp.wind.XWind)
    assert points.core.get('v').meta.is_same(gp.wind.YWind)

    assert points2.core.get('u').meta.is_same(gp.wind.EastWind)
    assert points2.core.get('v').meta.is_same(gp.wind.NorthWind)

def test_unproject_gridded():
    Proj = GriddedSkeleton.add_datavar([gp.wind.XWind('u'), gp.wind.YWind('v')]).add_magnitude(gp.wind.Wind)

    points = Proj(lon=(10,12), lat=(50,52))
    x,y = points.xy()
    points = Proj(x=x, y=y, crs=points.proj.utm())
    points.set_spacing(nx=5, ny=8)
    points.set_ff(10)
    points.set_dd(180)

    UnProj = Proj.unproject_class()

    points2 = UnProj.from_ds(points.ds())
    np.testing.assert_array_almost_equal(points.ff(), points2.ff())
    np.testing.assert_array_almost_equal(points.dd(), points2.dd())
    np.testing.assert_array_almost_equal(points.u(wrt='lonlat'), points2.u())
    np.testing.assert_array_almost_equal(points.v(wrt='lonlat'), points2.v())
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points.u(), 0)
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points.v(), 10)
    np.testing.assert_array_almost_equal(points2.u(),0)
    np.testing.assert_array_almost_equal(points2.v(),10)

    assert points.core.get('u').meta.is_same(gp.wind.XWind)
    assert points.core.get('v').meta.is_same(gp.wind.YWind)

    assert points2.core.get('u').meta.is_same(gp.wind.EastWind)
    assert points2.core.get('v').meta.is_same(gp.wind.NorthWind)

def test_project_point():
    UnProj = PointSkeleton.add_datavar([gp.wind.EastWind('u'), gp.wind.NorthWind('v')]).add_magnitude(gp.wind.Wind)

    points =UnProj(lon=(10,11,12), lat=(50,51,52))
    x,y = points.xy()
    points = UnProj(x=x, y=y, crs=points.proj.utm())
    points.set_ff(10)
    points.set_dd(180)

    Proj = UnProj.project_class()

    points2 = Proj.from_ds(points.ds())
    np.testing.assert_array_almost_equal(points.ff(), points2.ff())
    np.testing.assert_array_almost_equal(points.dd(), points2.dd())
    np.testing.assert_array_almost_equal(points2.u(wrt='lonlat'), points.u())
    np.testing.assert_array_almost_equal(points2.v(wrt='lonlat'), points.v())
    np.testing.assert_array_almost_equal(points2.u(), points.u(wrt='crs'))
    np.testing.assert_array_almost_equal(points2.v(), points.v(wrt='crs'))
    np.testing.assert_array_almost_equal(points.u(),0)
    np.testing.assert_array_almost_equal(points.v(),10)
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points2.u(), 0)
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points2.v(), 10)

    assert points.core.get('u').meta.is_same(gp.wind.EastWind)
    assert points.core.get('v').meta.is_same(gp.wind.NorthWind)

    assert points2.core.get('u').meta.is_same(gp.wind.XWind)
    assert points2.core.get('v').meta.is_same(gp.wind.YWind)


def test_project_gridded():
    UnProj = GriddedSkeleton.add_datavar([gp.wind.EastWind('u'), gp.wind.NorthWind('v')]).add_magnitude(gp.wind.Wind)

    points = UnProj(lon=(10,12), lat=(50,52))
    x,y = points.xy()
    points = UnProj(x=x, y=y, crs=points.proj.utm())
    points.set_ff(10)
    points.set_dd(180)

    Proj = UnProj.project_class()

    points2 = Proj.from_ds(points.ds())
    np.testing.assert_array_almost_equal(points.ff(), points2.ff())
    np.testing.assert_array_almost_equal(points.dd(), points2.dd())
    np.testing.assert_array_almost_equal(points2.u(wrt='lonlat'), points.u())
    np.testing.assert_array_almost_equal(points2.v(wrt='lonlat'), points.v())
    np.testing.assert_array_almost_equal(points2.u(), points.u(wrt='crs'))
    np.testing.assert_array_almost_equal(points2.v(), points.v(wrt='crs'))
    np.testing.assert_array_almost_equal(points.u(),0)
    np.testing.assert_array_almost_equal(points.v(),10)
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points2.u(), 0)
    np.testing.assert_raises(AssertionError, np.testing.assert_array_almost_equal, points2.v(), 10)

    assert points.core.get('u').meta.is_same(gp.wind.EastWind)
    assert points.core.get('v').meta.is_same(gp.wind.NorthWind)

    assert points2.core.get('u').meta.is_same(gp.wind.XWind)
    assert points2.core.get('v').meta.is_same(gp.wind.YWind)