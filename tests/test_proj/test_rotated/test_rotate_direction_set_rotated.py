from geo_skeletons.classes import WindGrid
import numpy as np

def test_reproj_southerly_winds_rotated_grid_lon0_0():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)

    wind.set_ff(10)
    wind.set_dd(180)
    rot = wind.dd(rotated=True)

    


    ind = np.where(wind.lon()==180)[0][0]
    np.testing.assert_almost_equal(rot[ind],0)

    ind = np.where(wind.lon()==0)[0][0]
    np.testing.assert_almost_equal(rot[ind],180)
    
        
    ind = np.where(wind.lon()==90)[0][0]
    np.testing.assert_almost_equal(rot[ind],90)
    
    
    ind = np.where(wind.lon()==-90)[0][0]
    np.testing.assert_almost_equal(rot[ind],270)

    x, y = wind.xy()
    ind = np.argmin(x)

    np.testing.assert_almost_equal(rot[ind],270)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot[ind],90)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot[ind],180)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot[ind],0)

