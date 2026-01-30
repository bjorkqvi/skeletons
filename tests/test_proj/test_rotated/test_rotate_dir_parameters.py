from geo_skeletons import GriddedSkeleton
import numpy as np
import geo_parameters as gp

def test_reproj_southerly_winds_rotated_grid_lon0_0():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = GriddedSkeleton.add_datavar(gp.wind.WindDir('dd'))(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    
    wind.set_dd(180)
    

    rot = wind.dd(rotated=True)

    # import matplotlib.pyplot as plt
    # #plt.scatter(x,y,c=lon,s=2)
    # x,y = wind.xy()
    # plt.scatter(x,y,c=rot,s=10, label='u')

    # plt.colorbar()


    # plt.show()


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

def test_reproj_southerly_winds_rotated_grid_lon0_0_dir_to():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = GriddedSkeleton.add_datavar(gp.wind.WindDir('dd'))(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    
    wind.set_dd(180)
    rot = wind.dd(rotated=True, dir_type='to')

    ind = np.where(wind.lon()==180)[0][0]
    np.testing.assert_almost_equal(rot[ind],180)

    ind = np.where(wind.lon()==0)[0][0]
    np.testing.assert_almost_equal(rot[ind],0)
    
        
    ind = np.where(wind.lon()==90)[0][0]
    np.testing.assert_almost_equal(rot[ind],270)
    
    
    ind = np.where(wind.lon()==-90)[0][0]
    np.testing.assert_almost_equal(rot[ind],90)

    x, y = wind.xy()
    ind = np.argmin(x)

    np.testing.assert_almost_equal(rot[ind],90)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot[ind],270)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot[ind],0)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot[ind],180)


def test_reproj_southerly_winds_rotated_grid_lon0_20():
    proj4 = "+proj=laea +lat_0=90 +lon_0=-20 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = GriddedSkeleton.add_datavar(gp.wind.WindDir('dd'))(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    
    wind.set_dd(180)
    rot = wind.dd(rotated=True)

    ind = np.where(wind.lon()==180-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],0)

    ind = np.where(wind.lon()==0-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],180)
    
        
    ind = np.where(wind.lon()==90-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],90)
    
    
    ind = np.where(wind.lon()==-90-20)[0][0]
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


def test_reproj_southerly_winds_rotated_grid_lon0_20_dir_to():
    proj4 = "+proj=laea +lat_0=90 +lon_0=-20 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = GriddedSkeleton.add_datavar(gp.wind.WindDir('dd'))(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    
    wind.set_dd(180)
    rot = wind.dd(rotated=True, dir_type='to')

    ind = np.where(wind.lon()==180-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],180)

    ind = np.where(wind.lon()==0-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],0)
    
        
    ind = np.where(wind.lon()==90-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],270)
    
    
    ind = np.where(wind.lon()==-90-20)[0][0]
    np.testing.assert_almost_equal(rot[ind],90)

    x, y = wind.xy()
    ind = np.argmin(x)

    np.testing.assert_almost_equal(rot[ind],90)

    ind = np.argmax(x)
    np.testing.assert_almost_equal(rot[ind],270)

    ind = np.argmin(y)
    np.testing.assert_almost_equal(rot[ind],0)

    ind = np.argmax(y)
    np.testing.assert_almost_equal(rot[ind],180)


def test_reproj_southerly_winds_rotated_grid_utm_u():
    wind = GriddedSkeleton.add_datavar(gp.wind.WindDir('dd'))(lon=(20,20.2), lat=(58,58.1))
    wind.set_spacing(nx=181)
    
    wind.set_dd(180)
    assert wind.proj.crs() == (34, 'V')

    rot = wind.dd(rotated=True) 
    # Rotation should be minimal in a small area
    np.testing.assert_array_almost_equal(wind.dd(), rot, decimal=0)