from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_coord
import numpy as np


def test_point_add_gp_trivial_ind():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=0, y=4, z=[0, 1, 2])
    assert set(points.coord_squeeze([])) == set({})
    assert set(points.coord_squeeze(["z"])) == {"z"}
    assert set(points.coord_squeeze(["inds"])) == {"inds"}
    assert set(points.coord_squeeze(["inds", "z"])) == {"z"}


def test_point_add_gp_trivial_all():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=0, y=4, z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["inds"])) == {"inds"}
    assert set(points.coord_squeeze(["inds", "z"])) == {"inds"}


def test_point_add_gp_no_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=[0, 2], y=[3, 4], z=[4, 5, 6])
    assert set(points.coord_squeeze([])) == set({})
    assert set(points.coord_squeeze(["z"])) == {"z"}
    assert set(points.coord_squeeze(["inds"])) == {"inds"}
    assert set(points.coord_squeeze(["inds", "z"])) == {"inds", "z"}


def test_gridded_add_gp_trivial_all():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=4, z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["y"])) == {"y"}
    assert set(points.coord_squeeze(["x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x"])) == {"y"}
    assert set(points.coord_squeeze(["y", "x", "z"])) == {"y"}

    points = Expanded(lon=0, lat=4, z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["lat"])) == {"lat"}
    assert set(points.coord_squeeze(["lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon"])) == {"lat"}
    assert set(points.coord_squeeze(["lat", "lon", "z"])) == {"lat"}


def test_gridded_add_gp_trivial_x():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=[4, 5], z=[1, 2])
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["y"])) == {"y"}
    assert set(points.coord_squeeze(["x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x"])) == {"y"}
    assert set(points.coord_squeeze(["y", "x", "z"])) == {"y", "z"}

    points = Expanded(lon=0, lat=[4, 5], z=[1, 2])
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["lat"])) == {"lat"}
    assert set(points.coord_squeeze(["lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon"])) == {"lat"}
    assert set(points.coord_squeeze(["lat", "lon", "z"])) == {"lat", "z"}


def test_gridded_add_gp_trivial_y():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(y=0, x=[4, 5], z=[1, 2])
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["y"])) == {"y"}
    assert set(points.coord_squeeze(["x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x", "z"])) == {"x", "z"}

    points = Expanded(lat=0, lon=[4, 5], z=[1, 2])
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["lat"])) == {"lat"}
    assert set(points.coord_squeeze(["lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon", "z"])) == {"lon", "z"}


def test_gridded_add_gp_trivial_x_z():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=[4, 5], z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["y"])) == {"y"}
    assert set(points.coord_squeeze(["x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x"])) == {"y"}
    assert set(points.coord_squeeze(["y", "x", "z"])) == {"y"}

    points = Expanded(lon=0, lat=[4, 5], z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["lat"])) == {"lat"}
    assert set(points.coord_squeeze(["lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon"])) == {"lat"}
    assert set(points.coord_squeeze(["lat", "lon", "z"])) == {"lat"}


def test_gridded_add_gp_trivial_y_z():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(y=0, x=[4, 5], z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["y"])) == {"y"}
    assert set(points.coord_squeeze(["x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x"])) == {"x"}
    assert set(points.coord_squeeze(["y", "x", "z"])) == {"x"}

    points = Expanded(lat=0, lon=[4, 5], z=1)
    assert set(points.coord_squeeze(["z"])) == set({"z"})
    assert set(points.coord_squeeze(["lat"])) == {"lat"}
    assert set(points.coord_squeeze(["lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon"])) == {"lon"}
    assert set(points.coord_squeeze(["lat", "lon", "z"])) == {"lon"}
