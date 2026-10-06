from geo_skeletons.classes import WindGrid
from geo_skeletons import GriddedSkeleton
import geo_parameters as gp
import numpy as np

def test_reproj_southerly_winds_rotated_grid_lon0_0():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180))
    wind.set_spacing(nx=181)

    wind.set_ff(10)
    wind.set_dd(180)

    rot = wind.dd(wrt=proj4)
    unrot = wind.dd()

    wind.set_dd(rot)
    np.testing.assert_array_almost_equal(wind.dd(), rot) # Takes them "as is"

    wind.set_dd(rot, wrt=proj4)

    np.testing.assert_array_almost_equal(wind.dd(wrt=proj4), rot)
    np.testing.assert_array_almost_equal(wind.dd(), unrot)



def test_reproj_southerly_winds_rotated_grid_lon0_0_dir_variable():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = GriddedSkeleton.add_time().add_datavar([gp.wind.Wind, gp.wind.WindDir])(lat=10, lon=(-180, 180), time=("2020-01-01 00:00", "2020-01-01 23:00"))
    wind.set_spacing(nx=181)

    wind.set_ff(10)
    wind.set_dd(180)

    rot = wind.dd(wrt=proj4)
    unrot = wind.dd()

    wind.set_dd(rot)
    np.testing.assert_array_almost_equal(wind.dd(), rot) # Takes them "as is"
    wind.set_dd(rot, wrt=proj4)
    np.testing.assert_array_almost_equal(wind.dd(wrt=proj4), rot)
    np.testing.assert_array_almost_equal(wind.dd(), unrot)
