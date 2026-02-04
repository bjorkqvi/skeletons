from geo_skeletons import GriddedSkeleton
from geo_skeletons import distance_funcs
import pytest
import numpy as np
# This area is small enough and within one UTM zone to test
RLON =(26.21,27.37)
RLAT = (4.33,5.29)
X = (440892.10517494264, 559107.8948250574)
Y = (6429147.6117879255, 6651832.7361561125)
DX = 0.04
DY = 0.04
DMX = 4443
DMY = 4461
NX = (RLON[1]-RLON[0])/DX +1 
NY = (RLAT[1]-RLAT[0])/DY +1 
DLON = 0.07705719812155304
DLAT = DMY/111_000
#DLAT = (LAT[1]-LAT[0])/(NY-1)
PROJ4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'

def test_dx_dy():
    data = GriddedSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dx(),DX)
    np.testing.assert_almost_equal(data.dy(),DY)
    np.testing.assert_almost_equal(data.dx(strict=True),DX)
    np.testing.assert_almost_equal(data.dy(strict=True),DY)
    np.testing.assert_almost_equal(data.dx(native=True),DX)
    np.testing.assert_almost_equal(data.dy(native=True),DY)
    
def test_dmx_dmy():
    data = GriddedSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dmx(),DMX, decimal=0)
    np.testing.assert_almost_equal(data.dmy(),DMY, decimal=0)
    assert data.dmx(strict=True) is None
    assert data.dmy(strict=True) is None
    np.testing.assert_almost_equal(data.dmx(native=True),DX)
    np.testing.assert_almost_equal(data.dmy(native=True),DY)

def test_dlon_dlat():
    data = GriddedSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    data.set_spacing(nx=NX, ny=NY)
    lon, lat = data.lonlat()
    lon = np.median(lon)
    lat = np.median(lat)
    dlon = distance_funcs.dx_to_dlon(DMX,lat, lon)
    np.testing.assert_almost_equal(data.dlon(),dlon, decimal=3)
    np.testing.assert_almost_equal(data.dlat(),DLAT, decimal=3)
    data.dlon(strict=True) is None
    data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True),DX)
    np.testing.assert_almost_equal(data.dlat(native=True),DY)