from geo_skeletons import PointSkeleton, GriddedSkeleton

from geo_skeletons.distance_funcs import distance_2points

import numpy as np
LON =   np.array([ -64.043,  -63.576,  -63.34 ,  -63.103,  -62.865,  -62.625,
        -62.384,  -62.142,  -61.899,  -61.654,  -61.408,  -61.161,
        -60.912,  -60.662,  -60.411,  -60.158,  -59.905,  -59.649,
        -59.393,  -59.135,  -58.876,  -55.668,  -55.392,  -55.115,
        -54.837,  -54.557,  -54.276,  -53.994,  -53.711,  -53.427,
        -53.141,  -52.854,  -52.566,  -52.276,  -51.986,  -51.694,
        -51.401,  -51.107,  -50.812,  -50.515,  -50.218,  -49.919,
        -49.619,  -49.318,  -49.016,  -48.713,  -48.408,  -48.103,
        -47.797,  -47.489,  -47.18 ,  -46.871,  -46.56 ,  -46.249,
        -45.936,  -45.622,  -45.308,  -44.992,  -44.676,  -44.359,
        -44.04 ,  -43.721,  -43.401,  -43.08 ,  -42.759,  -42.436,
        -42.113,  -41.789,  -41.464,  -41.138,  -40.812,  -40.485,
        -40.157,  -39.829,  -39.5  ,  -39.17 ,  -38.84 ,  -38.509,
        -38.177,  -37.845,  -37.513,  -37.179,  -36.846,  -36.512,
        -36.177,  -35.842,  -35.507,  -35.171,  -34.835,  -34.499,
        -34.162,  -33.825,  -33.488,  -33.151,  -32.813,  -32.475,
        -32.137,  -31.799,  -31.46 ,  -31.122,  -30.783,  -30.445,
        -30.106,  -29.768,  -29.429,  -29.09 ,  -28.752,  -28.413,
        -28.075,  -27.737,  -27.399,  -27.061,  -26.724,  -26.386,
        -26.049,  -25.712,  -25.376,  -25.039,  -24.703,  -24.368,
        -24.033,  -23.698,  -23.364,  -23.03 ,  -22.696,  -22.363,
        -22.031,  -21.699,  -21.368,  -21.037,  -20.707,  -20.378,
        -20.049,  -19.721,  -19.393,  -19.066,  -18.74 ,  -18.415,
        -18.09 ,  -17.766,  -17.443,  -17.121,  -16.8  ,  -16.479,
        -16.16 ,  -15.841,  -15.523,  -15.206,  -14.89 ,  -14.575,
        -14.26 ,  -13.947,  -13.635,  -13.324,  -13.014,  -12.704,
        -12.396,  -12.089,  -11.783,  -11.478,  -11.174,  -10.871,
        -10.57 ,  -10.269,   -9.97 ,   -9.671,   -9.374,   -9.078,
         -8.783,   -8.489,   -8.197,   -7.906,   -7.616,   -7.327,
         -7.039,   -6.752,   -6.467,   -6.183,   -5.9  ,   -5.619,
         -5.339,   -5.059,   -4.782,   -4.505,   -4.23 ,   -3.956,
         -3.683,   -3.412,   -3.142,   -2.873,   -2.605,   -2.339,
         -2.074,   -1.81 , -134.473, -134.83 , -135.185, -135.54 ,
       -135.893, -136.245, -136.596, -136.945, -137.293, -137.64 ,
       -137.986, -138.33 , -138.673, -139.014, -139.354, -139.693,
       -140.03 , -140.365, -140.7  , -141.033, -141.364, -141.694,
       -142.022, -142.349, -142.674, -142.998, -143.32 , -143.641,
       -143.96 , -144.277, -144.593, -144.908, -145.221, -145.532,
       -145.841, -146.149, -146.456, -146.76 , -147.063, -147.365,
       -147.665, -147.963, -148.259, -148.554, -148.848, -149.139,
       -149.429, -149.717, -150.004, -150.289, -150.573, -150.854,
       -151.134, -151.413, -151.69 , -151.965, -152.238, -152.51 ,
       -152.78 , -153.049, -153.316, -153.581, -153.845, -154.107,
       -154.368, -154.627, -154.884, -155.14 , -155.394, -155.646,
       -155.897, -156.147, -156.394, -156.641, -156.885, -157.128,
       -157.37 , -157.61 , -157.848, -158.034, -158.221, -158.409,
       -158.599, -158.791, -158.984, -159.179, -159.376, -159.574,
       -159.774, -159.976, -160.179, -160.384, -160.591, -160.8  ,
       -161.01 , -161.222, -161.436, -161.652, -161.869, -162.089,
       -162.31 , -162.533, -162.759, -162.986, -163.215, -163.446,
       -163.679, -163.914, -164.151, -164.39 , -164.631, -164.874,
       -165.119, -165.366, -165.616, -165.868, -166.121, -166.377,
       -166.635, -166.896, -167.158, -167.423, -167.69 , -167.96 ,
       -168.232, -168.506, -168.782, -169.061, -169.342, -169.625,
       -169.911, -170.2  , -170.49 , -170.784, -171.079, -171.378,
       -171.678, -171.981, -172.287, -172.595, -172.906, -173.22 ,
       -173.536, -173.854, -174.175, -174.499, -174.826, -175.155,
       -175.487, -175.821, -176.158, -176.498, -176.84 , -177.185,
       -177.533, -177.884, -178.237, -178.593, -178.952, -179.313,
       -179.677,  179.956,  179.586,  179.214,  178.839,  178.461,
        178.081,  177.697,  177.311,  176.923,  176.532,  176.137,
        175.741,  175.341,  174.939,  174.535,  174.127])
LAT = np.array([46.335, 46.551, 46.658, 46.765, 46.87 , 46.976, 47.08 , 47.184,
       47.288, 47.39 , 47.492, 47.594, 47.695, 47.795, 47.894, 47.993,
       48.091, 48.188, 48.285, 48.381, 48.476, 49.562, 49.647, 49.731,
       49.815, 49.898, 49.98 , 50.061, 50.142, 50.221, 50.3  , 50.377,
       50.454, 50.53 , 50.605, 50.679, 50.753, 50.825, 50.896, 50.967,
       51.036, 51.105, 51.173, 51.239, 51.305, 51.37 , 51.433, 51.496,
       51.558, 51.619, 51.678, 51.737, 51.795, 51.852, 51.907, 51.962,
       52.016, 52.068, 52.12 , 52.17 , 52.219, 52.268, 52.315, 52.361,
       52.406, 52.45 , 52.493, 52.535, 52.575, 52.615, 52.653, 52.69 ,
       52.727, 52.762, 52.796, 52.828, 52.86 , 52.89 , 52.92 , 52.948,
       52.975, 53.001, 53.025, 53.049, 53.071, 53.092, 53.112, 53.131,
       53.149, 53.165, 53.181, 53.195, 53.208, 53.219, 53.23 , 53.239,
       53.247, 53.254, 53.26 , 53.265, 53.268, 53.27 , 53.271, 53.271,
       53.27 , 53.267, 53.263, 53.258, 53.252, 53.245, 53.236, 53.226,
       53.215, 53.203, 53.19 , 53.175, 53.159, 53.142, 53.124, 53.105,
       53.085, 53.063, 53.04 , 53.016, 52.991, 52.965, 52.937, 52.909,
       52.879, 52.848, 52.816, 52.783, 52.749, 52.713, 52.677, 52.639,
       52.6  , 52.56 , 52.519, 52.477, 52.434, 52.389, 52.344, 52.297,
       52.25 , 52.201, 52.151, 52.1  , 52.049, 51.996, 51.942, 51.887,
       51.831, 51.774, 51.715, 51.656, 51.596, 51.535, 51.473, 51.41 ,
       51.346, 51.281, 51.214, 51.147, 51.079, 51.011, 50.941, 50.87 ,
       50.798, 50.725, 50.652, 50.577, 50.502, 50.426, 50.349, 50.27 ,
       50.192, 50.112, 50.031, 49.95 , 49.867, 49.784, 49.7  , 49.615,
       49.53 , 49.443, 49.356, 49.268, 49.179, 49.089, 48.999, 48.908,
       48.816, 48.723, 56.121, 56.069, 56.016, 55.962, 55.906, 55.849,
       55.791, 55.732, 55.671, 55.61 , 55.547, 55.483, 55.418, 55.352,
       55.285, 55.216, 55.147, 55.076, 55.004, 54.931, 54.858, 54.783,
       54.707, 54.629, 54.551, 54.472, 54.392, 54.311, 54.229, 54.145,
       54.061, 53.976, 53.89 , 53.803, 53.715, 53.626, 53.536, 53.445,
       53.354, 53.261, 53.167, 53.073, 52.978, 52.882, 52.785, 52.687,
       52.588, 52.488, 52.388, 52.287, 52.185, 52.082, 51.979, 51.874,
       51.769, 51.664, 51.557, 51.45 , 51.342, 51.233, 51.124, 51.013,
       50.903, 50.791, 50.679, 50.566, 50.452, 50.338, 50.223, 50.108,
       49.992, 49.875, 49.758, 49.64 , 49.521, 49.402, 49.283, 49.162,
       49.042, 49.197, 49.353, 49.508, 49.663, 49.818, 49.972, 50.126,
       50.28 , 50.434, 50.587, 50.74 , 50.893, 51.045, 51.197, 51.349,
       51.5  , 51.651, 51.802, 51.952, 52.102, 52.252, 52.401, 52.55 ,
       52.698, 52.846, 52.994, 53.141, 53.287, 53.434, 53.579, 53.725,
       53.87 , 54.014, 54.158, 54.301, 54.444, 54.586, 54.728, 54.869,
       55.01 , 55.15 , 55.29 , 55.429, 55.567, 55.705, 55.842, 55.979,
       56.115, 56.25 , 56.384, 56.518, 56.651, 56.784, 56.916, 57.047,
       57.177, 57.307, 57.436, 57.564, 57.691, 57.817, 57.943, 58.068,
       58.192, 58.315, 58.437, 58.559, 58.679, 58.799, 58.918, 59.035,
       59.152, 59.268, 59.383, 59.497, 59.61 , 59.722, 59.833, 59.942,
       60.051, 60.159, 60.265, 60.371, 60.475, 60.578, 60.681, 60.781,
       60.881, 60.98 , 61.077, 61.173, 61.268, 61.362, 61.454, 61.545,
       61.635, 61.723, 61.81 ])
def test_yank_one_point_with_exact_coordinate():
    grid = PointSkeleton(lon=(10, 11), lat=(0, 1))
    yanked_points = grid.yank_point(lon=10, lat=0)
    assert len(yanked_points["inds"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds"][0] == 0
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0)


def test_yank_several_points_with_exact_coordinates():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(0, 1, 2, 3, 4))
    yanked_points = grid.yank_point(lon=(10, 12, 14), lat=(0, 2, 4), fast=True)
    assert len(yanked_points["inds"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds"], np.array([0, 2, 4]))
    np.testing.assert_array_almost_equal(yanked_points["dx"], np.array([0, 0, 0]))


def test_yank_one_point_with_close_coordinate():
    grid = PointSkeleton(lon=(10, 11), lat=(0, 5))
    yanked_points = grid.yank_point(lon=10, lat=0.01)
    assert len(yanked_points["inds"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds"][0] == 0
    np.testing.assert_almost_equal(
        int(yanked_points["dx"][0]), int(distance_2points(0, 10, 0.01, 10))
    )


def test_yank_several_points_with_close_coordinates():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(0, 1, 2, 3, 3.5))
    yanked_points = grid.yank_point(
        lon=(10.001, 12, 13.01), lat=(0, 2.001, 3.001), fast=True
    )
    assert len(yanked_points["inds"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds"], np.array([0, 2, 3]))
    expected_dx = np.array(
        [
            distance_2points(0, 10, 0, 10.001),
            distance_2points(2, 12, 2.001, 12),
            distance_2points(3.0, 13, 3.001, 13.01),
        ]
    )
    np.testing.assert_array_almost_equal(
        (0.1 * yanked_points["dx"]).astype(int), (0.1 * expected_dx).astype(int)
    )


def test_yank_cartesian_point_from_spherical_grid():
    data = PointSkeleton(lon=(9.0, 9.1, 11.0), lat=(60.0, 60.9, 61.0))
    data.proj.set((33, "N"))

    dd = data.yank_point(x=165640, y=6666593)
    assert dd["inds"][0] == 0
    assert dd["dx"][0] < 1

    dd = data.yank_point(x=283749, y=6769393)
    assert dd["inds"][0] == 2
    assert dd["dx"][0] < 1


def test_yank_point_over84lat_in_list():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(0, 30, 40, 80, 86))
    yanked_points_fast = grid.yank_point(lon=13.5, lat=81, fast=True)
    yanked_points = grid.yank_point(lon=13.5, lat=81, fast=False)

    np.testing.assert_array_almost_equal(
        yanked_points["inds"], yanked_points_fast["inds"]
    )
    np.testing.assert_array_almost_equal(yanked_points["dx"], yanked_points_fast["dx"])


def test_yank_point_over84lat():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(0, 30, 40, 80, 86))
    yanked_points_fast = grid.yank_point(lon=13.5, lat=85, fast=True)
    yanked_points = grid.yank_point(lon=13.5, lat=85, fast=False)

    np.testing.assert_array_almost_equal(
        yanked_points["inds"], yanked_points_fast["inds"]
    )
    np.testing.assert_array_almost_equal(yanked_points["dx"], yanked_points_fast["dx"])


def test_yank_point_under80lat_in_list():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(-81, 30, 40, 80, 81))
    yanked_points_fast = grid.yank_point(lon=13.2, lat=81.1, fast=True)
    yanked_points = grid.yank_point(lon=13.2, lat=81.1, fast=False)

    np.testing.assert_array_almost_equal(
        yanked_points["inds"], yanked_points_fast["inds"]
    )
    np.testing.assert_array_almost_equal(yanked_points["dx"], yanked_points_fast["dx"])


def test_yank_point_under80lat():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(-79, 30, 40, 80, 86))
    yanked_points_fast = grid.yank_point(lon=10.5, lat=-81, fast=True)
    yanked_points = grid.yank_point(lon=10.5, lat=-81, fast=False)

    np.testing.assert_array_almost_equal(
        yanked_points["inds"], yanked_points_fast["inds"]
    )
    np.testing.assert_array_almost_equal(yanked_points["dx"], yanked_points_fast["dx"])


def test_yank_point_several_points():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(20, 30, 40, 50, 60))
    yanked_points = grid.yank_point(lon=(10.5, 13), lat=(25, 55), npoints=2)
    inds = yanked_points["inds"]
    np.testing.assert_array_almost_equal(
        np.array([10, 11, 13, 14]), grid.sel(inds=inds).lon()
    )
    np.testing.assert_array_almost_equal(
        np.array([20, 30, 50, 60]), grid.sel(inds=inds).lat()
    )


def test_yank_point_all_points():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(20, 30, 40, 50, 60))
    yanked_points = grid.yank_point(
        lon=(10.5, 13), lat=(25, 55), npoints=5, unique=True
    )
    inds = yanked_points["inds"]
    np.testing.assert_array_almost_equal(
        np.array([10, 11, 12, 13, 14]), grid.sel(inds=inds).lon()
    )
    np.testing.assert_array_almost_equal(
        np.array([20, 30, 40, 50, 60]), grid.sel(inds=inds).lat()
    )

def test_yank_point_lon_ambiguity():
    grid = PointSkeleton(lon=(175, -179), lat=(20, 20))
    yp = grid.yank_point(lon=180, lat=20)
    assert len(yp['inds']) == 1
    assert yp['inds'][0] == 1
    np.testing.assert_almost_equal(yp['dx'][0],104694, decimal=0)

def test_yank_point_complex_lonlat():
    all_points = GriddedSkeleton(lon=(-179.8, 179.999999999999), lat=(46.2, 61.900000000000006) )
    all_points.set_spacing(nx=500, ny=500)

    lon, lat = all_points.lonlat()
    all_points = PointSkeleton(lon=lon, lat=lat)
    
    yp = all_points.yank_point(lon=LON, lat=LAT)
    ii = yp['inds']
    # Using one UTM zone for all points lead to some points showing up in this region
    # Looping through points and setting UTM zone per point fixed this
    assert not np.any(np.logical_and(lon[ii]>50, lon[ii]<100))
    #import matplotlib.pyplot as plt
    #plt.scatter(points.lon(), points.lat())
    #plt.scatter(lon, lat)
    #plt.scatter(lon[ii], lat[ii])
    #plt.show()
    #breakpoint()    