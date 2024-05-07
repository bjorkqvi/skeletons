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

    assert set(grid.core.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "x", "y"]
    )
    g_coords = grid.core.coords("grid")
    gp_coords = grid.core.coords("gridpoint")
    s_coords = grid.core.coords("spatial")
    a_coords = grid.core.coords("all")
    assert set(g_coords) == set(["trivial", "test", "x", "y"])
    assert set(s_coords) == set(["x", "y"])
    assert set(gp_coords) == set(["another_trivial", "another"])
    assert set(grid.coord_squeeze(g_coords)) == set(["test", "y", "x"])
    assert set(grid.coord_squeeze(gp_coords)) == set(["another"])
    assert set(grid.coord_squeeze(s_coords)) == set(["x", "y"])
    assert set(grid.coord_squeeze(a_coords)) == set(["test", "another", "x", "y"])


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

    assert set(grid.core.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "x", "y"]
    )
    g_coords = grid.core.coords("grid")
    gp_coords = grid.core.coords("gridpoint")
    s_coords = grid.core.coords("spatial")
    a_coords = grid.core.coords("all")
    assert set(g_coords) == set(["trivial", "test", "x", "y"])
    assert set(s_coords) == set(["x", "y"])
    assert set(gp_coords) == set(["another_trivial", "another"])
    assert set(grid.coord_squeeze(g_coords)) == set(["test", "y"])
    assert set(grid.coord_squeeze(gp_coords)) == set(["another"])
    assert set(grid.coord_squeeze(s_coords)) == set(["y"])
    assert set(grid.coord_squeeze(a_coords)) == set(["test", "another", "y"])


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

    assert set(grid.core.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "x", "y"]
    )
    g_coords = grid.core.coords("grid")
    gp_coords = grid.core.coords("gridpoint")
    s_coords = grid.core.coords("spatial")
    a_coords = grid.core.coords("all")
    assert set(g_coords) == set(["trivial", "test", "x", "y"])
    assert set(s_coords) == set(["x", "y"])
    assert set(gp_coords) == set(["another_trivial", "another"])
    assert set(grid.coord_squeeze(g_coords)) == set(["test"])
    assert set(grid.coord_squeeze(gp_coords)) == set(["another"])
    assert set(grid.coord_squeeze(s_coords)) == set(["y"])
    assert set(grid.coord_squeeze(a_coords)) == set(["test", "another"])


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

    assert set(grid.core.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "inds"]
    )
    g_coords = grid.core.coords("grid")
    gp_coords = grid.core.coords("gridpoint")
    s_coords = grid.core.coords("spatial")
    a_coords = grid.core.coords("all")
    assert set(g_coords) == set(["trivial", "test", "inds"])
    assert set(s_coords) == set(["inds"])
    assert set(gp_coords) == set(["another_trivial", "another"])
    assert set(grid.coord_squeeze(g_coords)) == set(["test", "inds"])
    assert set(grid.coord_squeeze(gp_coords)) == set(["another"])
    assert set(grid.coord_squeeze(s_coords)) == set(["inds"])
    assert set(grid.coord_squeeze(a_coords)) == set(["test", "another", "inds"])


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

    assert set(grid.core.coords("all")) == set(
        ["trivial", "test", "another", "another_trivial", "inds"]
    )
    g_coords = grid.core.coords("grid")
    gp_coords = grid.core.coords("gridpoint")
    s_coords = grid.core.coords("spatial")
    a_coords = grid.core.coords("all")
    assert set(g_coords) == set(["trivial", "test", "inds"])
    assert set(s_coords) == set(["inds"])
    assert set(gp_coords) == set(["another_trivial", "another"])
    assert set(grid.coord_squeeze(g_coords)) == set(
        [
            "test",
        ]
    )
    assert set(grid.coord_squeeze(gp_coords)) == set(["another"])
    assert set(grid.coord_squeeze(s_coords)) == set(["inds"])
    assert set(grid.coord_squeeze(a_coords)) == set(["test", "another"])
