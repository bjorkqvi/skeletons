from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_coord


def test_coords():
    @add_datavar("hs")
    @add_coord("another_trivial")
    @add_coord("another")
    @add_coord("test", grid_coord=True)
    @add_coord("trivial", grid_coord=True)
    class Expanded1(GriddedSkeleton):
        pass

    grid = Expanded1(
        x=(1, 2, 3),
        y=(4, 5, 6, 7, 8),
        trivial=0,
        test=(9, 10),
        another=(1, 2, 3, 4),
        another_trivial=1,
        chunks=None,
    )
    grid.core.coords("nonspatial")

    assert set(grid.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "x", "y"]
    )
    assert set(grid.coords("grid")) == set(["trivial", "test", "x", "y"])
    assert set(grid.coords("spatial")) == set(["x", "y"])
    assert set(grid.coords("gridpoint")) == set(["another_trivial", "another"])
    assert set(grid.coords("grid", squeeze=True)) == set(["test", "y", "x"])
    assert set(grid.coords("gridpoint", squeeze=True)) == set(["another"])
    assert set(grid.coords("spatial", squeeze=True)) == set(["x", "y"])
    assert set(grid.coords("all", squeeze=True)) == set(["test", "another", "x", "y"])


def test_coords_one_trivial_spatial():
    @add_datavar("hs")
    @add_coord("another_trivial")
    @add_coord("another")
    @add_coord("test", grid_coord=True)
    @add_coord("trivial", grid_coord=True)
    class Expanded1(GriddedSkeleton):
        pass

    grid = Expanded1(
        x=(1),
        y=(4, 5, 6, 7, 8),
        trivial=0,
        test=(9, 10),
        another=(1, 2, 3, 4),
        another_trivial=1,
        chunks=None,
    )

    assert set(grid.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "x", "y"]
    )
    assert set(grid.coords("grid")) == set(["trivial", "test", "x", "y"])
    assert set(grid.coords("spatial")) == set(["x", "y"])
    assert set(grid.coords("gridpoint")) == set(["another_trivial", "another"])
    assert set(grid.coords("grid", squeeze=True)) == set(["test", "y"])
    assert set(grid.coords("gridpoint", squeeze=True)) == set(["another"])
    assert set(grid.coords("spatial", squeeze=True)) == set(["y"])
    assert set(grid.coords("all", squeeze=True)) == set(["test", "another", "y"])


def test_coords_two_trivial_spatial():
    @add_datavar("hs")
    @add_coord("another_trivial")
    @add_coord("another")
    @add_coord("test", grid_coord=True)
    @add_coord("trivial", grid_coord=True)
    class Expanded1(GriddedSkeleton):
        pass

    grid = Expanded1(
        x=(1),
        y=(2),
        trivial=0,
        test=(9, 10),
        another=(1, 2, 3, 4),
        another_trivial=1,
        chunks=None,
    )

    assert set(grid.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "x", "y"]
    )
    assert set(grid.coords("grid")) == set(["trivial", "test", "x", "y"])
    assert set(grid.coords("spatial")) == set(["x", "y"])
    assert set(grid.coords("gridpoint")) == set(["another_trivial", "another"])
    assert set(grid.coords("grid", squeeze=True)) == set(["test"])
    assert set(grid.coords("gridpoint", squeeze=True)) == set(["another"])
    assert set(grid.coords("spatial", squeeze=True)) == set(["y", "x"])
    assert set(grid.coords("all", squeeze=True)) == set(["test", "another"])


def test_coords_inds():
    @add_datavar("hs")
    @add_coord("another_trivial")
    @add_coord("another")
    @add_coord("test", grid_coord=True)
    @add_coord("trivial", grid_coord=True)
    class Expanded1(PointSkeleton):
        pass

    grid = Expanded1(
        x=(1, 2, 3),
        y=(4, 5, 6),
        trivial=0,
        test=(9, 10),
        another=(1, 2, 3, 4),
        another_trivial=1,
        chunks=None,
    )

    assert set(grid.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "inds"]
    )
    assert set(grid.coords("grid")) == set(["trivial", "test", "inds"])
    assert set(grid.coords("spatial")) == set(["inds"])
    assert set(grid.coords("gridpoint")) == set(["another_trivial", "another"])
    assert set(grid.coords("grid", squeeze=True)) == set(["test", "inds"])
    assert set(grid.coords("gridpoint", squeeze=True)) == set(["another"])
    assert set(grid.coords("spatial", squeeze=True)) == set(["inds"])
    assert set(grid.coords("all", squeeze=True)) == set(["test", "another", "inds"])


def test_coords_inds_trivial():
    @add_datavar("hs")
    @add_coord("another_trivial")
    @add_coord("another")
    @add_coord("test", grid_coord=True)
    @add_coord("trivial", grid_coord=True)
    class Expanded1(PointSkeleton):
        pass

    grid = Expanded1(
        x=(1),
        y=(4),
        trivial=0,
        test=(9, 10),
        another=(1, 2, 3, 4),
        another_trivial=1,
        chunks=None,
    )

    assert set(grid.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "inds"]
    )
    assert set(grid.coords("grid")) == set(["trivial", "test", "inds"])
    assert set(grid.coords("spatial")) == set(["inds"])
    assert set(grid.coords("gridpoint")) == set(["another_trivial", "another"])
    assert set(grid.coords("grid", squeeze=True)) == set(
        [
            "test",
        ]
    )
    assert set(grid.coords("gridpoint", squeeze=True)) == set(["another"])
    assert set(grid.coords("spatial", squeeze=True)) == set(["inds"])
    assert set(grid.coords("all", squeeze=True)) == set(["test", "another"])
