from geo_skeletons import GriddedSkeleton, PointSkeleton
import pytest
import numpy as np
from geo_skeletons.distance_funcs import distance_2points
LON = (20,22)
LAT = (58,60)
#X = (440892.10517494264, 559107.8948250574)
#Y = (6429147.6117879255, 6651832.7361561125)
CRS = (34, 'V')
NX = 11
NY = 21
DX = PointSkeleton(lon=np.linspace(LON[0], LON[1], NX), lat=np.median(LAT)).dmx()
DY = PointSkeleton(lat=np.linspace(LAT[0], LAT[1], NY), lon=np.median(LON)).dmy()
DLON = (LON[1]-LON[0])/(NX-1)
DLAT = (LAT[1]-LAT[0])/(NY-1)


def test_dx_dy():
    data = GriddedSkeleton(lon=LON, lat=LAT)
    assert data.proj.utm() == CRS
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dx(),DX, decimal=0)
    np.testing.assert_almost_equal(data.dy(),DY, decimal=0)
    assert data.dx(strict=True) is None
    assert data.dy(strict=True) is None
    np.testing.assert_almost_equal(data.dx(native=True),DLON)
    np.testing.assert_almost_equal(data.dy(native=True),DLAT)

def test_dmx_dmy():
    data = GriddedSkeleton(lon=LON, lat=LAT)
    assert data.proj.utm() == CRS
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dmx(),DX, decimal=0)
    np.testing.assert_almost_equal(data.dmy(),DY, decimal=0)
    assert data.dmx(strict=True) is None
    assert data.dmy(strict=True) is None
    np.testing.assert_almost_equal(data.dmx(native=True),DLON)
    np.testing.assert_almost_equal(data.dmy(native=True),DLAT)


def test_dlon_dlat():
    data = GriddedSkeleton(lon=LON, lat=LAT)
    assert data.proj.utm() == CRS
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dlon(),DLON)
    np.testing.assert_almost_equal(data.dlat(),DLAT)
    np.testing.assert_almost_equal(data.dlon(strict=True),DLON)
    np.testing.assert_almost_equal(data.dlat(strict=True),DLAT)
    np.testing.assert_almost_equal(data.dlon(native=True),DLON)
    np.testing.assert_almost_equal(data.dlat(native=True),DLAT)
