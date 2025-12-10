from geo_skeletons.classes import WindGrid, Wind
from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np
from geo_skeletons.managers.resample_manager import (
    create_new_class,
    init_new_class_to_grid,
)
import geo_parameters as gp


def test_class_grid_grid_time():
    data = WindGrid.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )
    data.set_spacing(nx=11, ny=21)

    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)

    assert new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert new_cls.core.coords() == data.__class__.core.coords()


def test_class_grid_grid():
    data = WindGrid(lon=(10, 20), lat=(50, 60))
    data.set_spacing(nx=11, ny=21)

    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)

    assert new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert new_cls.core.coords() == data.__class__.core.coords()


def test_class_point_point_time():
    data = Wind.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)

    assert not new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert new_cls.core.coords() == data.__class__.core.coords()

def test_class_point_point():
    data = Wind(lon=(10, 20), lat=(50, 60))

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)

    assert not new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert new_cls.core.coords() == data.__class__.core.coords()


def test_class_grid_point_time():
    data = WindGrid.add_time().add_mask(
        "land", opposite_name="sea", default_value=False
    )(lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00"))
    data.set_spacing(nx=11, ny=21)

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)

    assert not new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert "x" in new_cls.core._added_vars.keys()
    assert "y" in new_cls.core._added_vars.keys()
    assert new_cls.core.coords() == ["time", "inds"]
    assert new_cls.core.masks() == ["land_mask", "sea_mask"]

def test_class_grid_point():
    data = WindGrid.add_mask(
        "land", opposite_name="sea", default_value=False
    )(lon=(10, 20), lat=(50, 60))
    data.set_spacing(nx=11, ny=21)

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)

    assert not new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert "x" in new_cls.core._added_vars.keys()
    assert "y" in new_cls.core._added_vars.keys()
    assert new_cls.core.coords() == ["inds"]
    assert new_cls.core.masks() == ["land_mask", "sea_mask"]



def test_class_point_grid_time():
    data = (
        Wind.add_time()
        .add_frequency(gp.wave.Freq)
        .add_direction(gp.wave.Dirs)(
            lon=(10, 20),
            lat=(50, 60),
            time=("2020-01-01 00:00", "2020-01-10 06:00"),
            freq=[0, 1],
            dirs=[1, 2, 3],
        )
    )
    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)

    assert new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert "x" not in new_cls.core._added_vars.keys()
    assert "y" not in new_cls.core._added_vars.keys()
    assert new_cls.core.coords() == ["time", "y", "x", "freq", "dirs"]
    assert GriddedSkeleton.core.data_vars() == []

def test_class_point_grid():
    data = (
        Wind.add_frequency(gp.wave.Freq)
        .add_direction(gp.wave.Dirs)(
            lon=(10, 20),
            lat=(50, 60),
            freq=[0, 1],
            dirs=[1, 2, 3],
        )
    )
    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)

    assert new_cls.is_gridded()
    assert new_cls.core.data_vars() == data.core.data_vars()
    assert "x" not in new_cls.core._added_vars.keys()
    assert "y" not in new_cls.core._added_vars.keys()
    assert new_cls.core.coords() == ["y", "x", "freq", "dirs"]
    assert GriddedSkeleton.core.data_vars() == []


def test_init_data_grid_grid_time():
    data = WindGrid.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )
    data.set_spacing(nx=11, ny=21)

    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)

    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )
    assert new_data.time(datetime=False) == data.time(datetime=False)


def test_init_data_grid_grid():
    data = WindGrid(lon=(10, 20), lat=(50, 60))
    data.set_spacing(nx=11, ny=21)

    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)

    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )


def test_init_data_point_point_time():
    data = Wind.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)
    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert not new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )
    assert new_data.time(datetime=False) == data.time(datetime=False)
def test_init_data_point_point():
    data = Wind(
        lon=(10, 20), lat=(50, 60)
    )

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)
    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert not new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )


def test_init_data_grid_point_time():
    data = WindGrid.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )
    data.set_spacing(nx=11, ny=21)

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)
    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert not new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )
    assert new_data.time(datetime=False) == data.time(datetime=False)

def test_init_data_grid_point():
    data = WindGrid(
        lon=(10, 20), lat=(50, 60)
    )
    data.set_spacing(nx=11, ny=21)

    new_grid = PointSkeleton(x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs())

    new_cls = create_new_class(data, new_grid)
    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert not new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )


def test_init_data_point_grid_time():
    data = Wind.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )
    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)
    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )
    assert new_data.time(datetime=False) == data.time(datetime=False)

def test_init_data_point_grid():
    data = Wind(
        lon=(10, 20), lat=(50, 60)
    )
    new_grid = GriddedSkeleton(
        x=data.edges("x"), y=data.edges("y"), crs=data.proj.crs()
    )
    new_grid.set_spacing(nx=10, ny=21)

    new_cls = create_new_class(data, new_grid)
    new_data = init_new_class_to_grid(new_cls, new_grid, data)

    assert new_data.is_gridded()
    np.testing.assert_array_almost_equal(
        new_data.lon(native=True), new_grid.lon(native=True)
    )
    np.testing.assert_array_almost_equal(
        new_data.lat(native=True), new_grid.lat(native=True)
    )
