from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_coord, add_datavar


def test_point_basic():
    assert PointSkeleton._coord_manager.coords("all") == ["inds"]
    assert PointSkeleton._coord_manager.coords("spatial") == ["inds"]
    assert PointSkeleton._coord_manager.data_vars() == []
    assert PointSkeleton._coord_manager.data_vars("all") == ["y", "x"]
    assert PointSkeleton._coord_manager.data_vars("spatial") == ["y", "x"]

    points = PointSkeleton(x=[1, 2], y=[2, 3])
    assert points._coord_manager.coords("all") == ["inds"]
    assert points._coord_manager.data_vars() == []

    points2 = PointSkeleton(lon=[1, 2], lat=[2, 3])
    assert points2._coord_manager.coords("spatial") == ["inds"]
    assert points2._coord_manager.data_vars("all") == ["lat", "lon"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton._coord_manager.coords("spatial") == ["inds"]
    assert PointSkeleton._coord_manager.data_vars() == []

    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("all") == ["y", "x"]


def test_point_added_coord():
    @add_coord(name="w")
    @add_coord(name="z", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    assert Expanded._coord_manager.coords("spatial") == ["inds"]
    assert Expanded._coord_manager.data_vars() == []
    assert Expanded._coord_manager.coords() == ["inds", "z", "w"]
    assert Expanded._coord_manager.coords("nonspatial") == ["z", "w"]
    assert Expanded._coord_manager.coords("grid") == ["inds", "z"]
    assert Expanded._coord_manager.coords("gridpoint") == ["w"]

    points = Expanded(x=[1, 2], y=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9])
    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("spatial") == ["y", "x"]
    assert points._coord_manager.coords() == ["inds", "z", "w"]
    assert points._coord_manager.coords("grid") == ["inds", "z"]
    assert points._coord_manager.coords("gridpoint") == ["w"]

    points2 = Expanded(lon=[1, 2], lat=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9])
    assert points2._coord_manager.coords("spatial") == ["inds"]
    assert points2._coord_manager.data_vars("spatial") == ["lat", "lon"]
    assert points2._coord_manager.coords("nonspatial") == ["z", "w"]
    assert points2._coord_manager.coords("grid") == ["inds", "z"]
    assert points2._coord_manager.coords("gridpoint") == ["w"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton._coord_manager.coords("spatial") == ["inds"]
    assert PointSkeleton._coord_manager.data_vars("spatial") == [
        "y",
        "x",
    ]
    assert PointSkeleton._coord_manager.coords() == ["inds"]
    assert PointSkeleton._coord_manager.coords("grid") == ["inds"]
    assert PointSkeleton._coord_manager.coords("gridpoint") == []

    assert Expanded._coord_manager.coords("spatial") == ["inds"]
    assert Expanded._coord_manager.data_vars("spatial") == ["y", "x"]
    assert Expanded._coord_manager.coords("nonspatial") == ["z", "w"]
    assert Expanded._coord_manager.coords("grid") == ["inds", "z"]
    assert Expanded._coord_manager.coords("gridpoint") == ["w"]

    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("spatial") == ["y", "x"]
    assert points._coord_manager.coords() == ["inds", "z", "w"]
    assert points._coord_manager.coords("grid") == ["inds", "z"]
    assert points._coord_manager.coords("gridpoint") == ["w"]


def test_point_added_var():
    @add_datavar(name="eta")
    class Expanded(PointSkeleton):
        pass

    assert Expanded._coord_manager.coords("spatial") == ["inds"]
    assert Expanded._coord_manager.data_vars("spatial") == ["y", "x"]
    assert Expanded._coord_manager.coords() == ["inds"]
    assert Expanded._coord_manager.coords("grid") == ["inds"]
    assert Expanded._coord_manager.coords("gridpoint") == []
    assert Expanded._coord_manager.data_vars() == ["eta"]

    points = Expanded(x=[1, 2], y=[2, 3])
    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("spatial") == ["y", "x"]
    assert points._coord_manager.coords() == ["inds"]
    assert points._coord_manager.coords("grid") == ["inds"]
    assert points._coord_manager.coords("gridpoint") == []
    assert points._coord_manager.data_vars() == ["eta"]

    points2 = Expanded(lon=[1, 2], lat=[2, 3])
    assert points2._coord_manager.coords("spatial") == ["inds"]
    assert points2._coord_manager.data_vars("spatial") == ["lat", "lon"]
    assert points2._coord_manager.coords("nonspatial") == []
    assert points2._coord_manager.coords("grid") == ["inds"]
    assert points2._coord_manager.coords("gridpoint") == []
    assert points2._coord_manager.data_vars() == ["eta"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton._coord_manager.coords("spatial") == ["inds"]
    assert PointSkeleton._coord_manager.data_vars("spatial") == [
        "y",
        "x",
    ]
    assert PointSkeleton._coord_manager.coords() == ["inds"]
    assert PointSkeleton._coord_manager.coords("grid") == ["inds"]
    assert PointSkeleton._coord_manager.coords("gridpoint") == []
    assert PointSkeleton._coord_manager.data_vars() == []

    assert Expanded._coord_manager.coords("spatial") == ["inds"]
    assert Expanded._coord_manager.data_vars("spatial") == ["y", "x"]
    assert Expanded._coord_manager.coords() == ["inds"]
    assert Expanded._coord_manager.coords("grid") == ["inds"]
    assert Expanded._coord_manager.coords("gridpoint") == []
    assert Expanded._coord_manager.data_vars() == ["eta"]

    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("spatial") == ["y", "x"]
    assert points._coord_manager.coords() == ["inds"]
    assert points._coord_manager.coords("grid") == ["inds"]
    assert points._coord_manager.coords("gridpoint") == []
    assert points._coord_manager.data_vars() == ["eta"]


def test_point_added_coord_and_var():
    @add_datavar(name="eta_spatial", coord_group="spatial")
    @add_datavar(name="eta_gridpoint", coord_group="gridpoint")
    @add_datavar(name="eta_grid", coord_group="grid")
    @add_datavar(name="eta_all", coord_group="all")
    @add_coord(name="w")
    @add_coord(name="z", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    assert Expanded._coord_manager.coords("spatial") == ["inds"]
    assert Expanded._coord_manager.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert Expanded._coord_manager.coords() == ["inds", "z", "w"]
    assert Expanded._coord_manager.coords("grid") == ["inds", "z"]
    assert Expanded._coord_manager.coords("gridpoint") == ["w"]
    assert Expanded._coord_manager.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]

    points = Expanded(x=[1, 2], y=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9, 10])
    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert points._coord_manager.coords("nonspatial") == ["z", "w"]
    assert points._coord_manager.coords("grid") == ["inds", "z"]
    assert points._coord_manager.coords("gridpoint") == ["w"]
    assert points._coord_manager.data_vars() == ["eta_all", "eta_grid", "eta_gridpoint"]

    assert points.eta_all(empty=True).shape == (2, 4, 5)
    assert points.eta_grid(empty=True).shape == (2, 4)
    assert points.eta_gridpoint(empty=True).shape == (5,)
    assert points.eta_spatial(empty=True).shape == (2,)

    points2 = Expanded(lon=[1, 2], lat=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9, 10])
    assert points2._coord_manager.coords("spatial") == ["inds"]
    assert points2._coord_manager.data_vars("spatial") == ["lat", "lon", "eta_spatial"]
    assert points2._coord_manager.coords() == ["inds", "z", "w"]
    assert points2._coord_manager.coords("grid") == ["inds", "z"]
    assert points2._coord_manager.coords("gridpoint") == ["w"]
    assert points2._coord_manager.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
    assert points2._coord_manager.data_vars("spatial") == ["lat", "lon", "eta_spatial"]

    assert points2.shape("eta_all") == (2, 4, 5)
    assert points2.shape("eta_grid") == (2, 4)
    assert points2.shape("eta_gridpoint") == (5,)
    assert points2.shape("eta_spatial") == (2,)

    # Check that deepcopy of coord_manager works and these are not altered
    assert PointSkeleton._coord_manager.coords("spatial") == ["inds"]
    assert PointSkeleton._coord_manager.data_vars("spatial") == ["y", "x"]
    assert PointSkeleton._coord_manager.coords() == ["inds"]
    assert PointSkeleton._coord_manager.coords("grid") == ["inds"]
    assert PointSkeleton._coord_manager.coords("gridpoint") == []
    assert PointSkeleton._coord_manager.data_vars() == []

    assert Expanded._coord_manager.coords("spatial") == ["inds"]
    assert Expanded._coord_manager.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert Expanded._coord_manager.coords("nonspatial") == ["z", "w"]
    assert Expanded._coord_manager.coords("grid") == ["inds", "z"]
    assert Expanded._coord_manager.coords("gridpoint") == ["w"]
    assert Expanded._coord_manager.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
    assert points._coord_manager.coords("spatial") == ["inds"]
    assert points._coord_manager.data_vars("spatial") == ["y", "x", "eta_spatial"]
    assert points._coord_manager.coords() == ["inds", "z", "w"]
    assert points._coord_manager.coords("grid") == ["inds", "z"]
    assert points._coord_manager.coords("gridpoint") == ["w"]
    assert points._coord_manager.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
