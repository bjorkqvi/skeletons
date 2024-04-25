from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import (
    add_coord,
    add_datavar,
    add_mask,
    add_time,
    add_magnitude,
)


def test_point():
    @add_magnitude("mag", x="gr", y="gr", direction="dir", dir_type="from")
    @add_mask(name="all", coord_group="all", opposite_name="n_all", default_value=1)
    @add_mask(
        name="gridpoint",
        coord_group="gridpoint",
        opposite_name="n_gridpoint",
        default_value=0,
    )
    @add_mask(name="grid", coord_group="grid", opposite_name="n_grid", default_value=0)
    @add_mask(
        name="spatial",
        coord_group="spatial",
        opposite_name="n_spatial",
        default_value=0,
    )
    @add_datavar(name="al", coord_group="all")
    @add_datavar(name="sptl", coord_group="spatial")
    @add_datavar(name="grp", coord_group="gridpoint")
    @add_datavar(name="gr", coord_group="grid")
    @add_time()
    @add_coord(name="gp", grid_coord=False)
    @add_coord(name="g", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    time = ["2020-01-01 00:00", "2020-01-01 01:00"]
    points = Expanded(
        x=(0, 1, 2, 3),
        y=(4, 5, 6, 7),
        g=(10, 20, 30, 40, 50),
        gp=(100, 200, 300),
        time=time,
    )
    print(points)  # test repr also

    assert points.size() == points.size("all")
    assert points.size("all") == (2, 4, 5, 3)  # time, inds, g, gp
    assert points.size("spatial") == (4,)  # inds
    assert points.size("grid") == (4, 5)  # inds, g
    assert points.size("gridpoint") == (2, 3)  # time, gp

    assert points.shape("gr") == points.size("grid")
    assert points.shape("mag") == points.size("grid")
    assert points.shape("dir") == points.size("grid")
    assert points.shape("grp") == points.size("gridpoint")
    assert points.shape("sptl") == points.size("spatial")
    assert points.shape("al") == points.size("all")

    assert points.shape("grid_mask") == points.size("grid")
    assert points.shape("gridpoint_mask") == points.size("gridpoint")
    assert points.shape("spatial_mask") == points.size("spatial")
    assert points.shape("all_mask") == points.size("all")

    assert points.shape("n_grid_mask") == points.size("grid")
    assert points.shape("n_gridpoint_mask") == points.size("gridpoint")
    assert points.shape("n_spatial_mask") == points.size("spatial")
    assert points.shape("n_all_mask") == points.size("all")


def test_gridded():
    @add_magnitude("mag", x="gr", y="gr", direction="dir", dir_type="from")
    @add_mask(name="all", coord_group="all", opposite_name="n_all", default_value=1)
    @add_mask(
        name="gridpoint",
        coord_group="gridpoint",
        opposite_name="n_gridpoint",
        default_value=0,
    )
    @add_mask(name="grid", coord_group="grid", opposite_name="n_grid", default_value=0)
    @add_mask(
        name="spatial",
        coord_group="spatial",
        opposite_name="n_spatial",
        default_value=0,
    )
    @add_datavar(name="al", coord_group="all")
    @add_datavar(name="sptl", coord_group="spatial")
    @add_datavar(name="grp", coord_group="gridpoint")
    @add_datavar(name="gr", coord_group="grid")
    @add_time(grid_coord=True)
    @add_coord(name="gp", grid_coord=False)
    @add_coord(name="g", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    time = ["2020-01-01 00:00", "2020-01-01 01:00"]
    points = Expanded(
        x=(0, 1, 2, 3),
        y=(4, 5, 6, 7, 8, 9),
        g=(10, 20, 30, 40, 50),
        gp=(100, 200, 300),
        time=time,
    )
    print(points)  # test repr also

    assert points.size() == points.size("all")
    assert points.size("all") == (2, 6, 4, 5, 3)  # time, y,x, g, gp
    assert points.size("spatial") == (6, 4)  # y,x
    assert points.size("grid") == (2, 6, 4, 5)  # time, y, x, g
    assert points.size("gridpoint") == (3,)  # gp

    assert points.shape("gr") == points.size("grid")
    assert points.shape("mag") == points.size("grid")
    assert points.shape("dir") == points.size("grid")
    assert points.shape("grp") == points.size("gridpoint")
    assert points.shape("sptl") == points.size("spatial")
    assert points.shape("al") == points.size("all")

    assert points.shape("grid_mask") == points.size("grid")
    assert points.shape("gridpoint_mask") == points.size("gridpoint")
    assert points.shape("spatial_mask") == points.size("spatial")
    assert points.shape("all_mask") == points.size("all")

    assert points.shape("n_grid_mask") == points.size("grid")
    assert points.shape("n_gridpoint_mask") == points.size("gridpoint")
    assert points.shape("n_spatial_mask") == points.size("spatial")
    assert points.shape("n_all_mask") == points.size("all")
