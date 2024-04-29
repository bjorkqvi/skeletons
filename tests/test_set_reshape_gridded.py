from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_coord, activate_dask
from geo_skeletons.errors import DataWrongDimensionError
import dask.array as da
import numpy as np
import pytest


def data_is_dask(data) -> bool:
    return hasattr(data, "chunks")


def test_trivial_dimension_explicit_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4), chunks="auto")
    data = np.zeros((1, 1, 4))
    data_wrong_dim = np.zeros((1, 4, 1))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)
    points.set_dummy(data_wrong_dim, coords=["lon", "z", "lat"])
    assert data_is_dask(points.dummy())

    points.dask.deactivate()
    points.set_dummy(data)
    assert not data_is_dask(points.dummy())
    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, coords=["lon", "z", "lat"])
    assert not data_is_dask(points.dummy())


def test_trivial_dimension_explicit_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4))
    data = da.from_array(np.zeros((1, 1, 4)))
    data_wrong_dim = da.from_array(np.zeros((1, 4, 1)))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_wrong_dim, coords=["lon", "z", "lat"])
    assert data_is_dask(points.dummy())

    points.dask.deactivate()
    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, coords=["lon", "z", "lat"])
    assert not data_is_dask(points.dummy())


def test_trivial_dimension_implicit_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4))
    points.dask.activate()
    data = np.zeros((1, 1, 4))
    data_wrong_dim = np.zeros((1, 4, 1))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data.squeeze(), allow_reshape=False)

    points.set_dummy(data.squeeze())
    assert data_is_dask(points.dummy())

    points.dask.deactivate()
    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data.squeeze(), allow_reshape=False)

    points.set_dummy(data.squeeze(), allow_reshape=True)
    assert not data_is_dask(points.dummy())


def test_trivial_dimension_implicit_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2, z=range(4), chunks="auto")
    data = da.from_array(np.zeros((1, 1, 4)))
    data_wrong_dim = da.from_array(np.zeros((1, 4, 1)))

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    points.set_dummy(data_wrong_dim)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data.squeeze(), allow_reshape=False)

    points.set_dummy(data.squeeze())
    assert data_is_dask(points.dummy())

    points.dask.deactivate()
    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_wrong_dim, allow_reshape=False)

    points.set_dummy(data_wrong_dim, allow_reshape=True)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data.squeeze())

    points.set_dummy(data.squeeze())
    assert not data_is_dask(points.dummy())


def test_transpose_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(5), lat=range(4))
    data = np.zeros((4, 5))
    data[0, :] = 1
    data_T = data.T

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T, allow_reshape=False)

    points.set_dummy(data_T, allow_transpose=True)
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data)

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data)


def test_transpose_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(5), lat=range(4))
    data = da.from_array(np.zeros((4, 5)))
    data[0, :] = 1
    data_T = data.T

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data)

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data)


def test_transpose_with_trivial_dim_starting_with_numpy():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(5), lat=range(4), z=0, chunks="auto")
    data = np.zeros((4, 5, 1))
    data[0, :, :] = 1
    data_T = data[:, :, 0].T
    data_T2 = np.zeros((1, 5, 4, 1))
    data_T2[0, :, :, 0] = data_T

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data.squeeze())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2)

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2, allow_transpose=True, allow_reshape=False)

    points.set_dummy(data_T2, allow_transpose=True)
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(squeeze=False), data)

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(squeeze=False), data)

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2)

    points.set_dummy(data_T2, allow_transpose=True)
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data.squeeze())


def test_transpose_with_trivial_dim_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(5), lat=range(4), z=0)
    points.dask.activate()
    data = da.from_array(np.zeros((4, 5, 1)))
    data[0, :, :] = 1
    data_T = data[:, :, 0].T
    data_T2 = da.from_array(np.zeros((1, 5, 4, 1)))
    data_T2[0, :, :, 0] = data_T

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data.squeeze())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2)

    points.set_dummy(data_T2, allow_transpose=True)
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(squeeze=False), data)

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, allow_transpose=True)
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(squeeze=False), data)

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2)

    points.set_dummy(data_T2, allow_transpose=True)
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data.squeeze())


def test_transpose_with_trivial_dim_explicit_starting_with_dask():
    """*A trivial dimension can expanded to. Dask and numpy uses different reshape functions."""

    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(5), lat=range(4), z=0)
    points.dask.activate()
    data = da.from_array(np.zeros((4, 5, 1)))
    data[0, :, :] = 1
    data_T = data[:, :, 0].T
    data_T2 = da.from_array(np.zeros((1, 5, 4, 1)))
    data_T2[0, :, :, 0] = data_T

    points.set_dummy(data)
    assert data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, coords=["lon", "lat"])
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(squeeze=False), data)

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2)

    points.set_dummy(data_T2, coords=["lon", "lat"])
    assert data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data.squeeze())

    points.dask.deactivate()

    points.set_dummy(data)
    assert not data_is_dask(points.dummy())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T)

    points.set_dummy(data_T, coords=["lon", "lat"])
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(), data.squeeze())

    with pytest.raises(DataWrongDimensionError):
        points.set_dummy(data_T2)

    points.set_dummy(data_T2, coords=["lon", "lat"])
    assert not data_is_dask(points.dummy())
    np.testing.assert_allclose(points.dummy(squeeze=False), data)
