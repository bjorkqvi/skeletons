from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar


def test_gridded_basic():
    assert GriddedSkeleton.core.coords("spatial") == ["y", "x"]
    assert GriddedSkeleton.core.data_vars("spatial") == []

    points = GriddedSkeleton(x=[1, 2], y=[2, 3])
    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == []

    points2 = GriddedSkeleton(lon=[1, 2], lat=[2, 3])
    assert points2.core.coords("spatial") == ["lat", "lon"]
    assert points2.core.data_vars("spatial") == []

    # Check that deepcopy of coord_manager works and these are not altered
    assert GriddedSkeleton.core.coords("spatial") == ["y", "x"]
    assert GriddedSkeleton.core.data_vars("spatial") == []

    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == []


def test_gridded_added_coord():
    @add_coord(name="w")
    @add_coord(name="z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    assert Expanded.core.coords("spatial") == ["y", "x"]
    assert Expanded.core.data_vars("spatial") == []
    assert Expanded.core.coords() == ["y", "x", "z", "w"]
    assert Expanded.core.coords("grid") == ["y", "x", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]

    points = Expanded(x=[1, 2], y=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9])
    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == []
    assert points.core.coords() == ["y", "x", "z", "w"]
    assert points.core.coords("grid") == ["y", "x", "z"]
    assert points.core.coords("gridpoint") == ["w"]

    points2 = Expanded(lon=[1, 2], lat=[2, 3], z=[1, 2, 3, 4], w=[6, 7, 8, 9])
    assert points2.core.coords("spatial") == ["lat", "lon"]
    assert points2.core.data_vars("spatial") == []
    assert points2.core.coords() == ["lat", "lon", "z", "w"]
    assert points2.core.coords("grid") == ["lat", "lon", "z"]
    assert points2.core.coords("gridpoint") == ["w"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert GriddedSkeleton.core.coords("spatial") == ["y", "x"]
    assert GriddedSkeleton.core.data_vars("spatial") == []
    assert GriddedSkeleton.core.coords() == ["y", "x"]
    assert GriddedSkeleton.core.coords("grid") == ["y", "x"]
    assert GriddedSkeleton.core.coords("gridpoint") == []

    assert Expanded.core.coords("spatial") == ["y", "x"]
    assert Expanded.core.data_vars("spatial") == []
    assert Expanded.core.coords() == ["y", "x", "z", "w"]
    assert Expanded.core.coords("grid") == ["y", "x", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]

    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == []
    assert points.core.coords() == ["y", "x", "z", "w"]
    assert points.core.coords("grid") == ["y", "x", "z"]
    assert points.core.coords("gridpoint") == ["w"]


def test_gridded_added_var():
    @add_datavar(name="eta")
    class Expanded(GriddedSkeleton):
        pass

    assert Expanded.core.coords("spatial") == ["y", "x"]
    assert Expanded.core.data_vars("spatial") == []
    assert Expanded.core.coords() == ["y", "x"]
    assert Expanded.core.coords("grid") == ["y", "x"]
    assert Expanded.core.coords("gridpoint") == []
    assert Expanded.core.data_vars() == ["eta"]

    points = Expanded(x=[1, 2], y=[2, 3])
    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == []
    assert points.core.coords() == ["y", "x"]
    assert points.core.coords("grid") == ["y", "x"]
    assert points.core.coords("gridpoint") == []
    assert points.core.data_vars() == ["eta"]

    points2 = Expanded(lon=[1, 2], lat=[2, 3])
    assert points2.core.coords("spatial") == ["lat", "lon"]
    assert points2.core.data_vars("spatial") == []
    assert points2.core.coords() == ["lat", "lon"]
    assert points2.core.coords("grid") == ["lat", "lon"]
    assert points2.core.coords("gridpoint") == []
    assert points2.core.data_vars() == ["eta"]

    # Check that deepcopy of coord_manager works and these are not altered
    assert GriddedSkeleton.core.coords("spatial") == ["y", "x"]
    assert GriddedSkeleton.core.data_vars("spatial") == []
    assert GriddedSkeleton.core.coords() == ["y", "x"]
    assert GriddedSkeleton.core.coords("grid") == ["y", "x"]
    assert GriddedSkeleton.core.coords("gridpoint") == []
    assert GriddedSkeleton.core.data_vars() == []

    assert Expanded.core.coords("spatial") == ["y", "x"]
    assert Expanded.core.data_vars("spatial") == []
    assert Expanded.core.coords() == ["y", "x"]
    assert Expanded.core.coords("grid") == ["y", "x"]
    assert Expanded.core.coords("gridpoint") == []
    assert Expanded.core.data_vars() == ["eta"]

    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == []
    assert points.core.coords() == ["y", "x"]
    assert points.core.coords("grid") == ["y", "x"]
    assert points.core.coords("gridpoint") == []
    assert points.core.data_vars() == ["eta"]


def test_gridded_added_coord_and_var():
    @add_datavar(name="eta_spatial", coord_group="spatial")
    @add_datavar(name="eta_gridpoint", coord_group="gridpoint")
    @add_datavar(name="eta_grid", coord_group="grid")
    @add_datavar(name="eta_all", coord_group="all")
    @add_coord(name="w")
    @add_coord(name="z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    assert Expanded.core.coords("spatial") == ["y", "x"]
    assert Expanded.core.data_vars("spatial") == ["eta_spatial"]
    assert Expanded.core.coords("nonspatial") == ["z", "w"]
    assert Expanded.core.coords("all") == ["y", "x", "z", "w"]
    assert Expanded.core.coords("grid") == ["y", "x", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]
    assert Expanded.core.data_vars("all") == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
        "eta_spatial",
    ]

    points = Expanded(x=[1, 2], y=[2, 3, 4], z=[1, 2, 3, 4], w=[6, 7, 8, 9, 10])
    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == ["eta_spatial"]
    assert points.core.coords("nonspatial") == ["z", "w"]
    assert points.core.coords("all") == ["y", "x", "z", "w"]
    assert points.core.coords("grid") == ["y", "x", "z"]
    assert points.core.coords("gridpoint") == ["w"]
    assert points.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
    assert points.eta_all(empty=True).shape == (3, 2, 4, 5)
    assert points.eta_grid(empty=True).shape == (3, 2, 4)
    assert points.eta_gridpoint(empty=True).shape == (5,)
    assert points.eta_spatial(empty=True).shape == (3, 2)

    points2 = Expanded(lon=[1, 2], lat=[2, 3, 4], z=[1, 2, 3, 4], w=[6, 7, 8, 9, 10])
    assert points2.core.coords("spatial") == ["lat", "lon"]
    assert points2.core.data_vars("spatial") == ["eta_spatial"]
    assert points2.core.coords("nonspatial") == ["z", "w"]
    assert points2.core.coords("all") == ["lat", "lon", "z", "w"]
    assert points2.core.coords("grid") == ["lat", "lon", "z"]
    assert points2.core.coords("gridpoint") == ["w"]
    assert points2.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]

    assert points2.shape("eta_all") == (3, 2, 4, 5)
    assert points2.shape("eta_grid") == (3, 2, 4)
    assert points2.shape("eta_gridpoint") == (5,)
    assert points2.shape("eta_spatial") == (3, 2)

    # Check that deepcopy of coord_manager works and these are not altered
    assert GriddedSkeleton.core.coords("spatial") == ["y", "x"]
    assert GriddedSkeleton.core.data_vars("spatial") == []
    assert GriddedSkeleton.core.coords() == ["y", "x"]
    assert GriddedSkeleton.core.coords("grid") == ["y", "x"]
    assert GriddedSkeleton.core.coords("gridpoint") == []
    assert GriddedSkeleton.core.data_vars() == []

    assert Expanded.core.coords("spatial") == ["y", "x"]
    assert Expanded.core.data_vars("spatial") == ["eta_spatial"]
    assert Expanded.core.coords("nonspatial") == ["z", "w"]
    assert Expanded.core.coords("all") == ["y", "x", "z", "w"]
    assert Expanded.core.coords("grid") == ["y", "x", "z"]
    assert Expanded.core.coords("gridpoint") == ["w"]
    assert Expanded.core.data_vars() == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
    ]
    assert points.core.coords("spatial") == ["y", "x"]
    assert points.core.data_vars("spatial") == ["eta_spatial"]
    assert points.core.coords("nonspatial") == ["z", "w"]
    assert points.core.coords("all") == ["y", "x", "z", "w"]
    assert points.core.coords("grid") == ["y", "x", "z"]
    assert points.core.coords("gridpoint") == ["w"]
    assert points.core.data_vars("all") == [
        "eta_all",
        "eta_grid",
        "eta_gridpoint",
        "eta_spatial",
    ]
