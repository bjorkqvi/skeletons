from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar, add_coord, activate_dask
from geo_skeletons.errors import DataWrongDimensionError
import dask.array as da
import numpy as np
import pytest


def data_is_dask(data) -> bool:
    return hasattr(data, "chunks")


def test_trivial_dimension_explicit_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4))
    data = np.zeros((1, 4))
    data_wrong_dim = np.zeros((4,))
    data_T = np.zeros((4, 1))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_T, coords=["z", "inds"])
    assert data_is_dask(points.dummy())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_T, coords=["z", "inds"])
    assert not data_is_dask(points.dummy())
    points.set_dummy(data_T, coords=["z", "inds"], chunks="auto", silent=False)
    assert data_is_dask(points.dummy())


def test_trivial_dimension_explicit_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4), chunks="auto")
    data = da.from_array(np.zeros((1, 4)))
    data_wrong_dim = da.from_array(np.zeros((4,)))
    data_T = da.from_array(np.zeros((4, 1)))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_T, coords=["z", "inds"])
    assert data_is_dask(points.dummy())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_T, coords=["z", "inds"], chunks="auto", silent=False)
    assert data_is_dask(points.dummy())
    assert data_is_dask(points.ds().dummy)


def test_trivial_dimension_implicit_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4))
    points.dask.activate()
    data = np.zeros((1, 4))
    data_wrong_dim = np.zeros((4,))
    data_T = np.zeros((4, 1))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T, allow_reshape=False)
    points.set_dummy(data_T, allow_reshape=True)
    assert data_is_dask(points.dummy())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T, allow_reshape=False)

    points.set_dummy(data_T, allow_reshape=True, silent=False)
    assert not data_is_dask(points.dummy())


def test_trivial_dimension_implicit_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4))
    points.dask.activate()
    data = da.from_array(np.zeros((1, 4)))
    data_wrong_dim = da.from_array(np.zeros((4,)))
    data_T = da.from_array(np.zeros((4, 1)))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert data_is_dask(points.dummy())

    points.set_dummy(data_T)
    points.set_dummy(data_T, allow_reshape=True)
    assert data_is_dask(points.dummy())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data_T)

    points.set_dummy(data_T, allow_reshape=True, silent=False)
    assert not data_is_dask(points.dummy())


def test_squeeze_trivial_dimension_implicit_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    @add_coord(name="w")
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4), w=0)
    data = np.zeros((1, 1, 4))
    data_wrong_dim = np.zeros((4,))
    data_wrong_dim2 = np.zeros((1, 1, 4, 1))
    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim2, allow_reshape=False)

    points.set_dummy(data_wrong_dim2, allow_reshape=True)
    assert data_is_dask(points.dummy())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_wrong_dim, allow_reshape=True, silent=False)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim2, allow_reshape=False)

    points.set_dummy(data_wrong_dim2, allow_reshape=True)
    assert not data_is_dask(points.dummy())


def test_squeeze_trivial_dimension_implicit_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @activate_dask(chunks="auto")
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    @add_coord(name="w")
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4), w=0)
    data = da.from_array(np.zeros((1, 1, 4)))
    data_wrong_dim = da.from_array(np.zeros((4,)))
    data_wrong_dim2 = da.from_array(np.zeros((1, 1, 4, 1)))
    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim2)

    points.set_dummy(data_wrong_dim2, allow_reshape=True)
    assert data_is_dask(points.dummy())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True, silent=False)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim2, allow_reshape=False)

    points.set_dummy(data_wrong_dim2, allow_reshape=True)
    assert not data_is_dask(points.dummy())
