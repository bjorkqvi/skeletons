from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar, add_time
import numpy as np
import geo_parameters as gp
import pytest


@pytest.fixture
def wind():
    @add_datavar(gp.wind.YWind("v"))
    @add_datavar(gp.wind.XWind("u"))
    class WindData(PointSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_u(5)
    data.set_v(6)

    return data


def test_point_cartesian():
    @add_coord(name="test")
    class AddedCoordinate(PointSkeleton):
        pass

    @add_coord(name="test2")
    class AnotherAddedCoordinate(PointSkeleton):
        pass

    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    np.testing.assert_array_almost_equal(grid2.x(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid2.y(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid2.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 1]))
    assert list(grid2.ds().coords) == ["inds", "test"]

    grid3 = AnotherAddedCoordinate.from_ds(grid2.ds(), test2=np.array([5, 6]))
    np.testing.assert_array_almost_equal(grid3.x(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid3.y(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid3.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid3.test2(), np.array([5, 6]))
    assert list(grid3.ds().coords) == ["inds", "test2"]


def test_point_spherical():
    @add_coord(name="test")
    class AddedCoordinate(PointSkeleton):
        pass

    grid = PointSkeleton(lon=(1, 2), lat=(0, 3))
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    np.testing.assert_array_almost_equal(grid2.lon(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid2.lat(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid2.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 1]))
    assert list(grid2.ds().coords) == ["inds", "test"]


def test_gridded_cartesian():
    @add_coord(name="test")
    class AddedCoordinate(GriddedSkeleton):
        pass

    grid = GriddedSkeleton(x=(1, 4), y=(0, 4))
    grid.set_spacing(nx=4, ny=5)
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    grid2.set_test_spacing(nx=3)
    np.testing.assert_array_almost_equal(grid2.x(), np.arange(4) + 1)
    np.testing.assert_array_almost_equal(grid2.y(), np.arange(5))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 0.5, 1]))
    assert set(grid2.ds().coords) == set(["x", "y", "test"])


def test_gridded_spherical():
    @add_coord(name="test")
    class AddedCoordinate(GriddedSkeleton):
        pass

    grid = GriddedSkeleton(lon=(1, 4), lat=(0, 4))
    grid.set_spacing(nx=4, ny=5)
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    grid2.set_test_spacing(nx=3)
    np.testing.assert_array_almost_equal(grid2.lon(), np.arange(4) + 1)
    np.testing.assert_array_almost_equal(grid2.lat(), np.arange(5))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 0.5, 1]))
    assert set(grid2.ds().coords) == set(["lon", "lat", "test"])


def test_name_preserved():
    @add_datavar(name="test")
    class AddedVar(GriddedSkeleton):
        pass

    grid = AddedVar(lon=(1, 4), lat=(0, 4), name="test_name")

    grid.set_spacing(nx=4, ny=5)
    grid.set_test(2)

    grid2 = grid.from_ds(grid.ds())
    assert grid2.name == grid.name


def test_add_dynamic_var():
    @add_datavar(name="test")
    class AddedVar(GriddedSkeleton):
        pass

    grid = AddedVar(lon=(1, 4), lat=(0, 4), name="test_name")
    grid.set_spacing(nx=4, ny=5)
    grid.set_test(2)
    grid2 = GriddedSkeleton.from_ds(grid.ds(), dynamic=True)
    assert grid.core.all_objects() == grid2.core.all_objects()


def test_add_dynamic_var_gp():
    @add_datavar("test")
    class AddedVar(GriddedSkeleton):
        pass

    grid = AddedVar(lon=(1, 4), lat=(0, 4), name="test_name")

    grid.set_spacing(nx=4, ny=5)
    grid.set_test(2)
    grid.meta.set({"standard_name": "sea_surface_wave_significant_height"}, "test")
    grid2 = GriddedSkeleton.from_ds(grid.ds(), dynamic=True)

    set(grid2.core.all_objects()) == ["lon", "lat", "hs"]
    grid2 = GriddedSkeleton.from_ds(grid.ds(), keep_ds_names=True, dynamic=True)
    set(grid2.core.all_objects()) == ["lon", "lat", "test"]

    assert grid2.ds().test.units == "m"


def test_not_add_extra_var_to_static():
    """If we have a static core, then do not add extra variables from a Dataset"""

    @add_datavar("test3")
    @add_datavar("test2")
    @add_datavar("test")
    class DsCreator(GriddedSkeleton):
        pass

    @add_datavar("test")
    class AddedVar(GriddedSkeleton):
        pass

    grid = AddedVar(lon=(1, 4), lat=(0, 4), name="test_name")
    grid.set_spacing(nx=4, ny=5)
    grid.set_test(2)

    grid2 = DsCreator(lon=grid.lon(), lat=grid.lat())
    grid2.set_test(6)
    grid2.set_test2(3)
    grid2.set_test3(5)
    grid3 = grid.from_ds(grid2.ds())

    assert "test" in grid3.core.data_vars()
    assert "test2" not in grid3.core.data_vars()
    assert "test3" not in grid3.core.data_vars()


def test_mag_dir_unstructured():
    @add_datavar(gp.wind.Wind)
    @add_datavar(gp.wind.WindDir)
    @add_time()
    class Wind(PointSkeleton):
        pass

    data = Wind(lon=0, lat=3, time=("2020-01-01 00:00", "2020-01-01 23:00"))
    data.set_ff(5.0)
    data.set_dd(80.0)
    data2 = PointSkeleton.add_time().from_ds(data.ds(), dynamic=True)


def test_core_aliases_doc_example1():
    @add_datavar("Hm0")
    class Hm0(GriddedSkeleton):
        pass

    @add_datavar("hs")
    class Hs(GriddedSkeleton):
        pass

    hm0 = Hm0(x=0, y=0)
    hm0.set_Hm0(10)

    hs = Hs.from_ds(hm0.ds(), core_aliases={"hs": "Hm0"})

    np.testing.assert_almost_equal(hs.hs(), 10)


def test_core_aliases_doc_example2():
    @add_datavar("Hm0")
    class Hm0(GriddedSkeleton):
        pass

    @add_datavar(gp.wave.Hs("hsig"))
    class Hs(GriddedSkeleton):
        pass

    hm0 = Hm0(x=0, y=0)
    hm0.set_Hm0(10)

    hs = Hs.from_ds(hm0.ds(), core_aliases={gp.wave.Hs: "Hm0"})
    np.testing.assert_almost_equal(hs.hsig(), 10)


def test_ds_aliases_doc_example1():
    @add_datavar("Hm0")
    class Hm0(GriddedSkeleton):
        pass

    class Hs(GriddedSkeleton):
        pass

    hm0 = Hm0(x=0, y=0)
    hm0.set_Hm0(10)

    hs = Hs.from_ds(hm0.ds(), ds_aliases={"Hm0": "hs"}, dynamic=True)
    np.testing.assert_almost_equal(hs.hs(), 10)


def test_ds_aliases_doc_example2():
    @add_datavar("Hm0")
    class Hm0(GriddedSkeleton):
        pass

    class Hs(GriddedSkeleton):
        pass

    hm0 = Hm0(x=0, y=0)
    hm0.set_Hm0(10)

    hs = Hs.from_ds(hm0.ds(), ds_aliases={"Hm0": gp.wave.Hs}, dynamic=True)
    np.testing.assert_almost_equal(hs.hs(), 10)
    assert hs.meta.get("hs").get("standard_name") == gp.wave.Hs.standard_name()


def test_ds_aliases_doc_example3():
    @add_datavar("Hm0")
    class Hm0(GriddedSkeleton):
        pass

    class Hs(GriddedSkeleton):
        pass

    hm0 = Hm0(x=0, y=0)
    hm0.set_Hm0(10)

    hs = Hs.from_ds(hm0.ds(), ds_aliases={"Hm0": gp.wave.Hs("hsig")}, dynamic=True)
    np.testing.assert_almost_equal(hs.hsig(), 10)
    assert hs.meta.get("hsig").get("standard_name") == gp.wave.Hs.standard_name()


def test_ds_aliases_doc_example4():
    @add_datavar("Hm0")
    class Hm0(GriddedSkeleton):
        pass

    class Hs(GriddedSkeleton):
        pass

    hm0 = Hm0(x=0, y=0)
    hm0.set_Hm0(10)

    hs = Hs.from_ds(
        hm0.ds(), ds_aliases={"Hm0": gp.wave.Hs}, keep_ds_names=True, dynamic=True
    )
    np.testing.assert_almost_equal(hs.Hm0(), 10)
    assert hs.meta.get("Hm0").get("standard_name") == gp.wave.Hs.standard_name()


def test_wind(wind):
    points = PointSkeleton.from_ds(wind.ds(), dynamic=True)

    assert "ff" in points.core.magnitudes()
    assert "dd" in points.core.directions()


def test_extra_aliases(wind):
    ds_aliases = {
        "u": gp.wind.XWind,
        "v": gp.wind.YWind,
        "ux": gp.wind.XWind,
        "uy": gp.wind.YWind,
    }
    points = PointSkeleton.from_ds(wind.ds(), dynamic=True, ds_aliases=ds_aliases)
    assert "ff" in points.core.magnitudes()
    assert "dd" in points.core.directions()
