from geo_skeletons import GriddedSkeleton
import pytest
import numpy as np
from geo_skeletons.distance_funcs import distance_2points
LON = (20,22)
LAT = (58,60)
MEDIAN_LAT = np.median(np.array(LAT))
MEDIAN_LON = np.median(np.array(LON))
#X = (440892.10517494264, 559107.8948250574)
#Y = (6429147.6117879255, 6651832.7361561125)
#CRS = (34, 'V')
NX = 11
NY = 21
DMX = distance_2points(MEDIAN_LAT, LON[0], MEDIAN_LAT, LON[1])/(NX-1)
DMY = distance_2points(LAT[0], MEDIAN_LON, LAT[1], MEDIAN_LON)/(NY-1)
DLON = (LON[1]-LON[0])/(NX-1)
DLAT = (LAT[1]-LAT[0])/(NY-1)
PROJ4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
data = GriddedSkeleton(lon=LON, lat=LAT, crs=PROJ4)
data.set_spacing(nx=NX, ny=NY)
RLON = data.sel(lat=MEDIAN_LAT).edges('x')
RLAT = data.sel(lon=MEDIAN_LON).edges('y')
DRLON = (RLON[1]-RLON[0])/(NX-1)
DRLAT = (RLAT[1]-RLAT[0])/(NY-1)

def test_dx_dy():
    data = GriddedSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dx(),DRLON)
    np.testing.assert_almost_equal(data.dy(),DRLAT)
    
    assert data.dx(strict=True) is None
    assert data.dy(strict=True) is None
    np.testing.assert_almost_equal(data.dx(native=True),DLON)
    np.testing.assert_almost_equal(data.dy(native=True),DLAT)

def test_dmx_dmy():
    data = GriddedSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dmx(),DMX)
    np.testing.assert_almost_equal(data.dmy(),DMY)
    assert data.dmx(strict=True) is None
    assert data.dmy(strict=True) is None
    np.testing.assert_almost_equal(data.dmx(native=True),DLON)
    np.testing.assert_almost_equal(data.dmy(native=True),DLAT)


def test_dlon_dlat():
    data = GriddedSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dlon(),DLON)
    np.testing.assert_almost_equal(data.dlat(),DLAT)
    np.testing.assert_almost_equal(data.dlon(strict=True),DLON)
    np.testing.assert_almost_equal(data.dlat(strict=True),DLAT)
    np.testing.assert_almost_equal(data.dlon(native=True),DLON)
    np.testing.assert_almost_equal(data.dlat(native=True),DLAT)
