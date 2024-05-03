from geo_skeletons.gridded_skeleton import GriddedSkeleton
import numpy as np
import utm


def test_utm_conversion():
    lon = np.array([3, 4, 5])
    lat = np.array([60, 60.5, 61])
    x, y, zone, letter = utm.from_latlon(lat, lon)

    grid = GriddedSkeleton(
        lon=lon, lat=lat
    )  # Discards "middle" points, so this is equal to lon=(3,5) etc.
    grid.set_spacing(nx=3, ny=3)
    assert grid.nx() == 3
    assert grid.ny() == 3
    assert grid.size() == (3, 3)

    lon_vec, lat_vec = np.meshgrid(lon, lat)
    lon_vec = lon_vec.ravel().astype(float)
    lat_vec = lat_vec.ravel().astype(float)

    np.testing.assert_array_almost_equal(grid.lon(), lon)
    np.testing.assert_array_almost_equal(grid.lat(), lat)
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(grid.lonlat(), (lon_vec, lat_vec))
    np.testing.assert_array_almost_equal(grid.lon(strict=True), lon)
    np.testing.assert_array_almost_equal(grid.lat(strict=True), lat)
    np.testing.assert_array_almost_equal(grid.lonlat(strict=True), (lon_vec, lat_vec))
    assert grid.x(strict=True) is None
    assert grid.y(strict=True) is None
    assert np.all(grid.xy(strict=True) == (None, None))

    grid.utm.set((zone, letter))
    assert grid.utm.zone() == (zone, letter)

    x_rot, __, __, __ = utm.from_latlon(
        np.median(lat), lon, force_zone_number=zone, force_zone_letter=letter
    )
    __, y_rot, __, __ = utm.from_latlon(
        lat, np.median(lon), force_zone_number=zone, force_zone_letter=letter
    )
    x_vec, y_vec, __, __ = utm.from_latlon(
        lat_vec, lon_vec, force_zone_number=zone, force_zone_letter=letter
    )

    np.testing.assert_array_almost_equal(grid.x(), x_rot)
    np.testing.assert_array_almost_equal(grid.y(), y_rot)
    np.testing.assert_array_almost_equal(grid.xy(), (x_vec, y_vec))

    # Make linearly spaced in cartesian, not in spherical
    x = np.linspace(x[0], x[-1], 3)
    y = np.linspace(y[0], y[-1], 3)

    x_vec, y_vec = np.meshgrid(x, y)
    x_vec = x_vec.ravel().astype(float)
    y_vec = y_vec.ravel().astype(float)

    lat_vec, lon_vec = utm.to_latlon(
        x_vec,
        y_vec,
        zone_number=zone,
        zone_letter=letter,
        strict=False,
    )

    __, lon_rot = utm.to_latlon(
        x,
        np.median(y),
        zone_number=zone,
        zone_letter=letter,
        strict=False,
    )

    lat_rot, __ = utm.to_latlon(
        np.median(x),
        y,
        zone_number=zone,
        zone_letter=letter,
        strict=False,
    )

    grid2 = GriddedSkeleton(x=x, y=y)
    grid2.set_spacing(nx=3, ny=3)
    grid2.utm.set((zone, letter))
    assert grid2.utm.zone() == (zone, letter)

    assert grid2.nx() == 3
    assert grid2.ny() == 3
    assert grid2.size() == (3, 3)

    assert grid2.lon(strict=True) is None
    assert grid2.lat(strict=True) is None
    assert np.all(grid2.lonlat(strict=True) == (None, None))

    np.testing.assert_array_almost_equal(grid2.x(strict=True), x)
    np.testing.assert_array_almost_equal(grid2.y(strict=True), y)

    np.testing.assert_array_almost_equal(grid2.xy(strict=True), (x_vec, y_vec))

    np.testing.assert_array_almost_equal(grid2.lon(), lon_rot, decimal=5)
    np.testing.assert_array_almost_equal(grid2.lat(), lat_rot, decimal=5)
    np.testing.assert_array_almost_equal(grid2.lonlat(), (lon_vec, lat_vec), decimal=5)
