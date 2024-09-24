from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_mask, add_datavar
import numpy as np


def test_trivial_mask():
    @add_mask("land", opposite_name="sea")
    class Dummy(PointSkeleton):
        pass

    points = Dummy(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    points.set_land_mask(True)

    lon, lat = points.land_points()
    np.testing.assert_array_almost_equal(points.lon(), lon)
    np.testing.assert_array_almost_equal(points.lat(), lat)

    lon, lat = points.get("land_points")
    np.testing.assert_array_almost_equal(points.lon(), lon)
    np.testing.assert_array_almost_equal(points.lat(), lat)

    lon, lat = points.sea_points()
    assert not np.any(lon)
    assert not np.any(lat)

    lon, lat = points.get("sea_points")
    assert not np.any(lon)
    assert not np.any(lat)


def test_triggered_land_mask():
    @add_mask(
        name="sea",
        coord_group="grid",
        default_value=1,
        opposite_name="land",
        triggered_by="topo",
        valid_range=(0, None),
        range_inclusive=False,
    )
    @add_datavar("topo")
    class Grid(GriddedSkeleton):
        pass

    grid = Grid(lon=(5, 6), lat=(59, 61))
    grid.set_spacing(nx=10, ny=10)
    grid.set_topo(10)
    topo = grid.topo()
    topo[:, 0:3] = 0
    topo[5, :] = 5
    grid.set_topo(topo)
    np.testing.assert_array_equal(grid.sea_mask(), grid.get("sea_mask"))
    np.testing.assert_array_equal(grid.land_mask(), grid.get("land_mask"))
    np.testing.assert_array_equal(
        grid.land_mask(), np.logical_not(grid.get("sea_mask"))
    )

    np.testing.assert_array_almost_equal(grid.sea_points(), grid.get("sea_points"))
    np.testing.assert_array_almost_equal(
        grid.sea_points(coord="x"), grid.get("sea_points", coord="x")
    )
    np.testing.assert_array_almost_equal(
        grid.sea_points(coord="x", utm=(33, "W")),
        grid.get("sea_points", coord="x", utm=(33, "W")),
    )
    np.testing.assert_array_almost_equal(grid.land_points(), grid.get("land_points"))
    np.testing.assert_array_almost_equal(
        grid.land_points(coord="x"), grid.get("land_points", coord="x")
    )
    np.testing.assert_array_almost_equal(
        grid.land_points(coord="x", utm=(33, "W")),
        grid.get("land_points", coord="x", utm=(33, "W")),
    )
