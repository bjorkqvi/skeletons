from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons import distance_funcs 
import numpy as np
import pytest
from geo_skeletons.errors import SkeletonError, ProjectionError
RLON =(26.21,27.37)
RLAT = (4.33,5.29)

CRS = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
# LON = (20,22)
# LAT = (58,60)
# X = (440892.10517494264, 559107.8948250574)
# Y = (6429147.6117879255, 6651832.7361561125)
# CRS = (34, 'V')
# NX = 11
# NY = 21
# DX = (X[1]-X[0])/(NX-1)
# DY = (Y[1]-Y[0])/(NY-1)
# DLON = (LON[1]-LON[0])/(NX-1)
# DLAT = (LAT[1]-LAT[0])/(NY-1)

def test_dx_dy():
   data = GriddedSkeleton(x=RLON, y=RLAT, crs=CRS)
   data.set_spacing(dx=DX, dy=DY)
   np.testing.assert_almost_equal(DX, data.dx())
   np.testing.assert_almost_equal(DY, data.dy())


def test_dmx_dmy():
   data = GriddedSkeleton(x=RLON, y=RLAT, crs=CRS)
   data.set_spacing(dmx=DX, dmy=DY)
   np.testing.assert_almost_equal(DX, data.dmx())
   np.testing.assert_almost_equal(DY, data.dmy())

def test_dmx_dmy():
   data = GriddedSkeleton(x=RLON, y=RLAT, crs=CRS)
   data.set_spacing(dm=DX)
   np.testing.assert_almost_equal(DX, data.dmx())
   np.testing.assert_almost_equal((Y[1]-Y[0])/(19), data.dmy())

def test_dlon_dlat():
   data = GriddedSkeleton(x=RLON, y=RLAT, crs=CRS)
   data.set_spacing(dlon=0.1, dlat=0.1)
   lon, lat = data.lonlat()
   dx = distance_funcs.dlon_to_dx(data.dlon(), np.median(lat), np.median(lon))
   dy = distance_funcs.dlat_to_dy(data.dlat(), np.median(lat), np.median(lon))
   np.testing.assert_almost_equal(dy, data.dmy(), decimal=0)
   np.testing.assert_almost_equal(dx, data.dmx(), decimal=0)
   np.testing.assert_almost_equal(0.1, data.dlon(), decimal=1)
   np.testing.assert_almost_equal(0.1, data.dlat(), decimal=1)
