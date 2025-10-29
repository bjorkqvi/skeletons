from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np


def test_add_time():
    TimePointSkeleton = PointSkeleton.add_time()
    TimePointSkeleton2 = PointSkeleton.add_time()
    assert TimePointSkeleton.core.coords("grid") == ["time", "inds"]
    assert PointSkeleton.core.coords("all") == ["inds"]
    assert isinstance(
        TimePointSkeleton(lon=0, lat=0, time=("2020-01-01 00:00")), TimePointSkeleton
    )
    assert not isinstance(
        TimePointSkeleton(lon=0, lat=0, time=("2020-01-01 00:00")), TimePointSkeleton2
    )

    TimeGriddedSkeleton = GriddedSkeleton.add_time()
    TimeGriddedSkeleton2 = GriddedSkeleton.add_time()
    assert TimeGriddedSkeleton.core.coords("grid") == ["time", "y", "x"]
    assert GriddedSkeleton.core.coords("all") == ["y", "x"]
    assert isinstance(
        TimeGriddedSkeleton(lon=0, lat=0, time=("2020-01-01 00:00")),
        TimeGriddedSkeleton,
    )
    assert not isinstance(
        TimeGriddedSkeleton(lon=0, lat=0, time=("2020-01-01 00:00")),
        TimeGriddedSkeleton2,
    )


def test_add_frequency():
    ModPointSkeleton = PointSkeleton.add_frequency()
    ModPointSkeleton2 = PointSkeleton.add_frequency()
    assert ModPointSkeleton.core.coords("all") == ["inds", "freq"]
    assert ModPointSkeleton.core.coords("gridpoint") == ["freq"]
    assert PointSkeleton.core.coords("all") == ["inds"]
    assert isinstance(
        ModPointSkeleton(lon=0, lat=0, freq=np.arange(3)), ModPointSkeleton
    )
    assert not isinstance(
        ModPointSkeleton(lon=0, lat=0, freq=np.arange(3)), ModPointSkeleton2
    )


def test_add_direction():
    ModPointSkeleton = PointSkeleton.add_direction()
    ModPointSkeleton2 = PointSkeleton.add_direction()
    assert ModPointSkeleton.core.coords("all") == ["inds", "dirs"]
    assert ModPointSkeleton.core.coords("gridpoint") == ["dirs"]
    assert PointSkeleton.core.coords("all") == ["inds"]
    assert isinstance(
        ModPointSkeleton(lon=0, lat=0, dirs=np.arange(3)), ModPointSkeleton
    )
    assert not isinstance(
        ModPointSkeleton(lon=0, lat=0, dirs=np.arange(3)), ModPointSkeleton2
    )


def test_add_coord():
    ModPointSkeleton = PointSkeleton.add_coord("z")
    ModPointSkeleton2 = PointSkeleton.add_coord("z", grid_coord=True)
    assert ModPointSkeleton.core.coords("all") == ["inds", "z"]
    assert ModPointSkeleton.core.coords("gridpoint") == ["z"]
    assert ModPointSkeleton2.core.coords("all") == ["inds", "z"]
    assert ModPointSkeleton2.core.coords("gridpoint") == []
    assert ModPointSkeleton2.core.coords("grid") == ["inds", "z"]

    assert PointSkeleton.core.coords("all") == ["inds"]
    assert isinstance(ModPointSkeleton(lon=0, lat=0, z=np.arange(3)), ModPointSkeleton)
    assert not isinstance(
        ModPointSkeleton(lon=0, lat=0, z=np.arange(3)), ModPointSkeleton2
    )
