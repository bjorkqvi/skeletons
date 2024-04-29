from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_coord, activate_dask
import dask.array as da
import numpy as np


def validate_chunks(chunks, dims, data):
    for n, key in enumerate(dims):
        assert np.median(data.chunksizes[key]) == chunks[n]


def test_set_chunks():
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(500), lat=range(400), z=range(100))
    points.set_dummy()
    chunks = (100, 100, 100)
    points.set_dummy(points.dummy(), chunks=chunks)
    points.dummy(data_array=True, dask=True)
    validate_chunks(
        chunks, ["lat", "lon", "z"], points.dummy(data_array=True, dask=True)
    )


def test_rechunk():
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(500), lat=range(400), z=range(100))
    points.dask.activate()
    points.set_dummy()
    chunks = (100, 100, 50)
    points.dask.rechunk(chunks)

    validate_chunks(chunks, ["lat", "lon", "z"], points.dummy(data_array=True))


def test_rechunk_using_dict():
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(lon=range(500), lat=range(400), z=range(100))
    points.dask.activate()
    points.set_dummy()
    chunks = {"z": 50, "lat": 80, "lon": 100}
    points.dask.rechunk(chunks)
    validate_chunks(list(chunks.values()), chunks.keys(), points.dummy(data_array=True))


def test_rechunk_primary_dim():
    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(x=range(500), y=range(400), z=range(100))
    points.set_dummy()
    points.dask.rechunk(primary_dim="z")
    validate_chunks([100], ["z"], points.dummy(data_array=True))

    points.dask.rechunk(primary_dim="x")
    validate_chunks([500], ["x"], points.dummy(data_array=True))

    points.dask.rechunk(primary_dim="y")
    validate_chunks([400], ["y"], points.dummy(data_array=True))


def test_rechunk_primary_dims():
    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(x=range(20), y=range(40), z=range(1000))
    points.set_dummy()
    points.dask.rechunk(primary_dim=["z", "x"])
    validate_chunks([1000, 20], ["z", "x"], points.dummy(data_array=True))

    points.dask.rechunk(primary_dim=["y", "z"])
    validate_chunks([40, 1000], ["y", "z"], points.dummy(data_array=True))

    points.dask.rechunk(primary_dim=["x", "y"])
    validate_chunks([20, 40], ["x", "y"], points.dummy(data_array=True))


def test_rechunk_set_method():
    @activate_dask()
    @add_datavar(name="dummy", default_value=-9)
    @add_coord(name="z")
    class DummySkeleton(GriddedSkeleton):
        pass

    points = DummySkeleton(x=range(20), y=range(40), z=range(1000))
    points.set_dummy()
    points.dask.rechunk(chunks=(5, 10, 20))
    validate_chunks([5, 10, 20], ["y", "x", "z"], points.dummy(data_array=True))
    points.set_dummy(chunks=(10, 20, 30))
    validate_chunks([10, 20, 30], ["y", "x", "z"], points.dummy(data_array=True))
