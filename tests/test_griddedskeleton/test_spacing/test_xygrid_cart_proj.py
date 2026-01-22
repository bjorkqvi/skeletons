from geo_skeletons import GriddedSkeleton
import pytest
import numpy as np
def test_get_xy_lonlat():
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).x(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).y(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lon(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lat(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).xy(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lonlat(strict=True, native=True)

# This area is small enough and within one UTM zone to test
LON = (20,22)
LAT = (58,60)
X = (440892.10517494264, 559107.8948250574)
Y = (6429147.6117879255, 6651832.7361561125)
CRS = (34, 'V')
NX = 11
NY = 21
DX = (X[1]-X[0])/(NX-1)
DY = (Y[1]-Y[0])/(NY-1)
DLON = (LON[1]-LON[0])/(NX-1)
DLAT = (LAT[1]-LAT[0])/(NY-1)

def test_dx_dy():
    data = GriddedSkeleton(x=X, y=Y, crs=CRS)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dx(),DX)
    np.testing.assert_almost_equal(data.dy(),DY)
    np.testing.assert_almost_equal(data.dx(strict=True),DX)
    np.testing.assert_almost_equal(data.dy(strict=True),DY)
    np.testing.assert_almost_equal(data.dx(native=True),DX)
    np.testing.assert_almost_equal(data.dy(native=True),DY)
    
def test_dmx_dmy():
    data = GriddedSkeleton(x=X, y=Y, crs=CRS)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dmx(),DX)
    np.testing.assert_almost_equal(data.dmy(),DY)
    np.testing.assert_almost_equal(data.dmx(strict=True),DX)
    np.testing.assert_almost_equal(data.dmy(strict=True),DY)
    np.testing.assert_almost_equal(data.dmx(native=True),DX)
    np.testing.assert_almost_equal(data.dmy(native=True),DY)

def test_dlon_dlat():
    data = GriddedSkeleton(x=X, y=Y, crs=CRS)
    data.set_spacing(nx=NX, ny=NY)
    np.testing.assert_almost_equal(data.dlon(),DLON, decimal=2)
    np.testing.assert_almost_equal(data.dlat(),DLAT, decimal=2)
    data.dlon(strict=True) is None
    data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True),DX)
    np.testing.assert_almost_equal(data.dlat(native=True),DY)