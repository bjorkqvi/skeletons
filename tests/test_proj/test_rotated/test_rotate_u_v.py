from geo_skeletons.classes import WindGrid
import numpy as np

def test_reproj_southerly_winds_rotated_grid_lon0_0_u():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)
    rot_u = wind.u(rotated=True) 
    assert rot_u[0] == rot_u[-1]

    ind = np.where(wind.lon()==180)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],0)

    ind = np.where(wind.lon()==0)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],0)
    
        
    ind = np.where(wind.lon()==90)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],-10)
    
    
    ind = np.where(wind.lon()==-90)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],10)

    x, y = wind.xy()
    ind = np.argmin(x)
    np.testing.assert_almost_equal(rot_u[ind],10)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot_u[ind],-10)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot_u[ind],0)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot_u[ind],0)

def test_reproj_southerly_winds_rotated_grid_lon0_0_v():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)
    rot_v = wind.v(rotated=True) 
    assert rot_v[0] == rot_v[-1]

    ind = np.where(wind.lon()==180)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],-10)

    ind = np.where(wind.lon()==0)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],10)
    
        
    ind = np.where(wind.lon()==90)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],0)
    
    
    ind = np.where(wind.lon()==-90)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],0)

    x, y = wind.xy()
    ind = np.argmin(x)
    np.testing.assert_almost_equal(rot_v[ind],0)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot_v[ind],0)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot_v[ind],10)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot_v[ind],-10)

def test_reproj_southerly_winds_rotated_grid_lon0_20_u():
    proj4 = "+proj=laea +lat_0=90 +lon_0=-20 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)
    rot_u = wind.u(rotated=True) 
    assert rot_u[0] == rot_u[-1]

    ind = np.where(wind.lon()==180-20)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],0)

    ind = np.where(wind.lon()==0-20)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],0)
    
        
    ind = np.where(wind.lon()==90-20)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],-10)
    
    
    ind = np.where(wind.lon()==-90-20)[0][0]
    np.testing.assert_almost_equal(rot_u[ind],10)

    x, y = wind.xy()
    ind = np.argmin(x)
    np.testing.assert_almost_equal(rot_u[ind],10)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot_u[ind],-10)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot_u[ind],0)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot_u[ind],0)


def test_reproj_southerly_winds_rotated_grid_lon0_20_v():
    proj4 = "+proj=laea +lat_0=90 +lon_0=-20 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)
    rot_v = wind.v(rotated=True) 
    assert rot_v[0] == rot_v[-1]

    ind = np.where(wind.lon()==180-20)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],-10)

    ind = np.where(wind.lon()==0-20)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],10)
    
        
    ind = np.where(wind.lon()==90-20)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],0)
    
    
    ind = np.where(wind.lon()==-90-20)[0][0]
    np.testing.assert_almost_equal(rot_v[ind],0)

    x, y = wind.xy()
    ind = np.argmin(x)
    np.testing.assert_almost_equal(rot_v[ind],0)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot_v[ind],0)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot_v[ind],10)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot_v[ind],-10)


def test_reproj_southerly_winds_rotated_grid_utm_u():
    wind = WindGrid(lon=(20,20.2), lat=(58,58.1))
    wind.set_spacing(nx=10, ny=20)
    wind.set_ff(10)
    wind.set_dd(180)
    assert wind.proj.crs() == (34, 'V')

    rot_u = wind.u(rotated=True) 
    rot_v = wind.v(rotated=True) 
    # Rotation should be minimal in a small area
    np.testing.assert_array_almost_equal(wind.u(), rot_u, decimal=1)
    np.testing.assert_array_almost_equal(wind.v(), rot_v, decimal=3)

    # breakpoint()
    # import matplotlib.pyplot as plt
    # #plt.scatter(x,y,c=lon,s=2)
    # x,y = wind.xy()
    # plt.scatter(x,y,c=rot_u,s=10, label='u')

    # plt.colorbar()
    

    # plt.show()
    # breakpoint()