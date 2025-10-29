from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_coord, add_datavar


def test_point_basic():
    assert PointSkeleton.core.coords("all") == ["inds"]
    assert PointSkeleton.core.coords("spatial") == ["inds"]
    assert PointSkeleton.core.data_vars() == []
    assert PointSkeleton.core.data_vars("all") == ["y", "x"]
    assert PointSkeleton.core.data_vars("spatial") == ["y", "x"]

    points = PointSkeleton(x=[1, 2], y=[2, 3])
    assert points.core.coords("all") == ["inds"]
    assert points.core.data_vars() == []

    points2 = PointSkeleton(lon=[1, 2], lat=[2, 3])
    assert points2.core.coords("spatial") == ["inds"]
    assert points2.core.data_vars("all") == ["lat", "lon"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton.core.coords("spatial") == ["inds"]
    assert PointSkeleton.core.data_vars() == []

    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("all") == ["y", "x"]


def test_point_added_coord():
    @add_coord(name="w")
    @add_coord(name="z", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    assert Expanded.core.coords("spatial") == ["inds"]
    assert Expanded.core.data_vars() == []
    assert Expanded.core.coords() == ["inds", "z", "w"]
    assert Expanded.core.coords("nonspatial") == ["z", "w"]
    assert Expanded.core.coords("grid") == ["inds", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]

    points = Expanded(x=[1, 2], y=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9])
    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("spatial") == ["y", "x"]
    assert points.core.coords() == ["inds", "z", "w"]
    assert points.core.coords("grid") == ["inds", "z"]
    assert points.core.coords("gridpoint") == ["w"]

    points2 = Expanded(lon=[1, 2], lat=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9])
    assert points2.core.coords("spatial") == ["inds"]
    assert points2.core.data_vars("spatial") == ["lat", "lon"]
    assert points2.core.coords("nonspatial") == ["z", "w"]
    assert points2.core.coords("grid") == ["inds", "z"]
    assert points2.core.coords("gridpoint") == ["w"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton.core.coords("spatial") == ["inds"]
    assert PointSkeleton.core.data_vars("spatial") == [
        "y",
        "x",
    ]
    assert PointSkeleton.core.coords() == ["inds"]
    assert PointSkeleton.core.coords("grid") == ["inds"]
    assert PointSkeleton.core.coords("gridpoint") == []

    assert Expanded.core.coords("spatial") == ["inds"]
    assert Expanded.core.data_vars("spatial") == ["y", "x"]
    assert Expanded.core.coords("nonspatial") == ["z", "w"]
    assert Expanded.core.coords("grid") == ["inds", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]

    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("spatial") == ["y", "x"]
    assert points.core.coords() == ["inds", "z", "w"]
    assert points.core.coords("grid") == ["inds", "z"]
    assert points.core.coords("gridpoint") == ["w"]


def test_point_added_var():
    @add_datavar(name="eta")
    class Expanded(PointSkeleton):
        pass

    assert Expanded.core.coords("spatial") == ["inds"]
    assert Expanded.core.data_vars("spatial") == ["y", "x"]
    assert Expanded.core.coords() == ["inds"]
    assert Expanded.core.coords("grid") == ["inds"]
    assert Expanded.core.coords("gridpoint") == []
    assert Expanded.core.data_vars() == ["eta"]

    points = Expanded(x=[1, 2], y=[2, 3])
    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("spatial") == ["y", "x"]
    assert points.core.coords() == ["inds"]
    assert points.core.coords("grid") == ["inds"]
    assert points.core.coords("gridpoint") == []
    assert points.core.data_vars() == ["eta"]

    points2 = Expanded(lon=[1, 2], lat=[2, 3])
    assert points2.core.coords("spatial") == ["inds"]
    assert points2.core.data_vars("spatial") == ["lat", "lon"]
    assert points2.core.coords("nonspatial") == []
    assert points2.core.coords("grid") == ["inds"]
    assert points2.core.coords("gridpoint") == []
    assert points2.core.data_vars() == ["eta"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton.core.coords("spatial") == ["inds"]
    assert PointSkeleton.core.data_vars("spatial") == [
        "y",
        "x",
    ]
    assert PointSkeleton.core.coords() == ["inds"]
    assert PointSkeleton.core.coords("grid") == ["inds"]
    assert PointSkeleton.core.coords("gridpoint") == []
    assert PointSkeleton.core.data_vars() == []

    assert Expanded.core.coords("spatial") == ["inds"]
    assert Expanded.core.data_vars("spatial") == ["y", "x"]
    assert Expanded.core.coords() == ["inds"]
    assert Expanded.core.coords("grid") == ["inds"]
    assert Expanded.core.coords("gridpoint") == []
    assert Expanded.core.data_vars() == ["eta"]

    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("spatial") == ["y", "x"]
    assert points.core.coords() == ["inds"]
    assert points.core.coords("grid") == ["inds"]
    assert points.core.coords("gridpoint") == []
    assert points.core.data_vars() == ["eta"]


def test_point_added_coord_and_var():
    @add_datavar(name="eta_spatial", coord_group="spatial")
    @add_datavar(name="eta_gridpoint", coord_group="gridpoint")
    @add_datavar(name="eta_grid", coord_group="grid")
    @add_datavar(name="eta_all", coord_group="all")
    @add_coord(name="w")
    @add_coord(name="z", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    assert Expanded.core.coords("spatial") == ["inds"]
    assert Expanded.core.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert Expanded.core.coords() == ["inds", "z", "w"]
    assert Expanded.core.coords("grid") == ["inds", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]
    assert Expanded.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]

    points = Expanded(x=[1, 2], y=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9, 10])
    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert points.core.coords("nonspatial") == ["z", "w"]
    assert points.core.coords("grid") == ["inds", "z"]
    assert points.core.coords("gridpoint") == ["w"]
    assert points.core.data_vars() == ["eta_all", "eta_grid", "eta_gridpoint"]

    assert points.eta_all(empty=True).shape == (2, 4, 5)
    assert points.eta_grid(empty=True).shape == (2, 4)
    assert points.eta_gridpoint(empty=True).shape == (5,)
    assert points.eta_spatial(empty=True).shape == (2,)

    points2 = Expanded(lon=[1, 2], lat=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9, 10])
    assert points2.core.coords("spatial") == ["inds"]
    assert points2.core.data_vars("spatial") == ["lat", "lon", "eta_spatial"]
    assert points2.core.coords() == ["inds", "z", "w"]
    assert points2.core.coords("grid") == ["inds", "z"]
    assert points2.core.coords("gridpoint") == ["w"]
    assert points2.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
    assert points2.core.data_vars("spatial") == ["lat", "lon", "eta_spatial"]

    assert points2.shape("eta_all") == (2, 4, 5)
    assert points2.shape("eta_grid") == (2, 4)
    assert points2.shape("eta_gridpoint") == (5,)
    assert points2.shape("eta_spatial") == (2,)

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton.core.coords("spatial") == ["inds"]
    assert PointSkeleton.core.data_vars("spatial") == ["y", "x"]
    assert PointSkeleton.core.coords() == ["inds"]
    assert PointSkeleton.core.coords("grid") == ["inds"]
    assert PointSkeleton.core.coords("gridpoint") == []
    assert PointSkeleton.core.data_vars() == []

    assert Expanded.core.coords("spatial") == ["inds"]
    assert Expanded.core.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert Expanded.core.coords("nonspatial") == ["z", "w"]
    assert Expanded.core.coords("grid") == ["inds", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]
    assert Expanded.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
    assert points.core.coords("spatial") == ["inds"]
    assert points.core.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert points.core.coords() == ["inds", "z", "w"]
    assert points.core.coords("grid") == ["inds", "z"]
    assert points.core.coords("gridpoint") == ["w"]
    assert points.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
