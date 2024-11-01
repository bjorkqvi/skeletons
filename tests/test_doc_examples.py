from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import (
    add_datavar,
    add_magnitude,
    add_coord,
    add_frequency,
    add_time,
    add_direction,
)
import numpy as np
import geo_parameters as gp


def test_yank():
    grid = GriddedSkeleton(lon=(3, 5), lat=(60, 61))
    grid.set_spacing(dm=1000)
    point_dict = grid.yank_point(lon=2.98, lat=60.01)
    assert point_dict["inds_x"][0] == 0
    assert point_dict["inds_y"][0] == 1

    points = PointSkeleton(lon=(3.0, 4.0, 5.0), lat=(60.0, 60.0, 61.0))
    point_dict = points.yank_point(lon=2.98, lat=60.01)
    assert point_dict["inds"][0] == 0

    point_dict = points.yank_point(lon=(2.98, 4.1), lat=(60.01, 60.01))
    assert point_dict["inds"][0] == 0
    assert point_dict["inds"][1] == 1

    point_dict = grid.yank_point(lon=2.98, lat=60.01, npoints=4)
    np.testing.assert_array_equal(
        point_dict["dx"].astype(int),
        np.array([1120.6812202, 1428.55452856, 1576.18628188, 2131.94091801]).astype(
            int
        ),
    )
    np.testing.assert_array_equal(point_dict["inds_x"], np.array([0, 0, 0, 1]))
    np.testing.assert_array_equal(point_dict["inds_y"], np.array([1, 2, 0, 1]))

    lon, lat = grid.lonlat()
    raveled_grid = PointSkeleton(lon=lon, lat=lat)
    point_dict = raveled_grid.yank_point(lon=2.98, lat=60.01, npoints=4)

    point_dict = raveled_grid.yank_point(
        lon=2.98, lat=60.01, npoints=4, gridded_shape=grid.size()
    )
    np.testing.assert_array_equal(point_dict["inds_x"], np.array([0, 0, 0, 1]))
    np.testing.assert_array_equal(point_dict["inds_y"], np.array([1, 2, 0, 1]))


def test_mag_dir():

    @add_magnitude(gp.wind.Wind("u"), x="ux", y="uy", direction=gp.wind.WindDir("ud"))
    @add_datavar(gp.wind.YWind("uy"))
    @add_datavar(gp.wind.XWind("ux"))
    class Wind(GriddedSkeleton):
        pass

    data = Wind(lon=(10, 14), lat=(50, 60))
    data.set_u(10)
    data.set_ud(45)

    @add_datavar(gp.wave.Dirp)
    class Wave(GriddedSkeleton):
        pass

    data = Wave(lon=(10, 14), lat=(50, 60))
    data.set_dirp(45, dir_type="to")


def test_wind():
    @add_datavar(gp.wind.Wind, default_value=10.0)
    @add_coord(name="z", grid_coord=True)
    class WindSpeed(GriddedSkeleton):
        pass

    grid = WindSpeed(lon=(25, 30), lat=(58, 62), z=(0, 100))
    grid.set_spacing(dnmi=1)
    grid.set_z_spacing(dx=1)

    new_data = np.random.rand(grid.ny(), grid.nx(), len(grid.z()))
    grid.set_ff(new_data)


def test_coord_groups():
    @add_datavar("roughness", default_value=0.0, coord_group="grid")
    @add_datavar(gp.wind.Wind, default_value=10.0, coord_group="all")
    @add_coord(name="z", grid_coord=False)
    class WindSpeed(GriddedSkeleton):
        pass


def test_wave_spec():
    @add_datavar(gp.wave.Efth("spec"), coord_group="all")
    @add_direction()
    @add_frequency()
    @add_time()
    class Spectrum(GriddedSkeleton):
        pass

    data = Spectrum(
        lon=(10, 20),
        lat=(50, 60),
        freq=np.arange(0, 1, 0.1),
        dirs=np.arange(0, 360, 10),
        time=("2020-01-01 00:00", "2020-01-02 00:00", "6h"),
    )

    for spec in data:
        pass
