from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
import utm


def test_basic_spherical_to_utm_point():
    points = PointSkeleton(lon=range(1, 6), lat=range(10, 60, 10))
    x, y, zone, letter = utm.from_latlon(points.lat(), points.lon())
    assert points.utm.zone() == (zone, letter)  # (31,'P')
    np.testing.assert_array_almost_equal(points.x(), x)
    np.testing.assert_array_almost_equal(points.y(), y)
    xx, yy = points.xy()
    np.testing.assert_array_almost_equal(xx, x)
    np.testing.assert_array_almost_equal(yy, y)

    x, y, zone, letter = utm.from_latlon(
        points.lat(), points.lon(), force_zone_number=33, force_zone_letter="W"
    )
    assert (zone, letter) == (33, "W")
    np.testing.assert_array_almost_equal(points.x(utm=(33, "W")), x)
    np.testing.assert_array_almost_equal(points.y(utm=(33, "W")), y)

    xx, yy = points.xy(utm=(33, "W"))
    np.testing.assert_array_almost_equal(xx, x)
    np.testing.assert_array_almost_equal(yy, y)

    assert points.utm.zone() == (31, "P")  # Should not change
    points.utm.set((33, "W"))
    assert points.utm.zone() == (33, "W")

    np.testing.assert_array_almost_equal(points.x(), x)
    np.testing.assert_array_almost_equal(points.y(), y)
    xx, yy = points.xy()
    np.testing.assert_array_almost_equal(xx, x)
    np.testing.assert_array_almost_equal(yy, y)

    np.testing.assert_array_almost_equal(points.x(utm=(33, "W")), x)
    np.testing.assert_array_almost_equal(points.y(utm=(33, "W")), y)
    xx, yy = points.xy(utm=(33, "W"))
    np.testing.assert_array_almost_equal(xx, x)
    np.testing.assert_array_almost_equal(yy, y)

    points.utm.set((32, "W"))
    xedge = points.edges("x", utm=(33, "W"))
    yedge = points.edges("y", utm=(33, "W"))
    np.testing.assert_almost_equal(np.min(x), xedge[0])
    np.testing.assert_almost_equal(np.max(x), xedge[1])
    np.testing.assert_almost_equal(np.min(y), yedge[0])
    np.testing.assert_almost_equal(np.max(y), yedge[1])


def test_basic_spherical_to_utm_gridded():
    points = GriddedSkeleton(lon=range(1, 6), lat=range(10, 60, 10))
    x, _, zone, letter = utm.from_latlon(
        np.median(points.lat()),
        points.lon(),
        force_zone_number=31,
        force_zone_letter="P",
    )
    assert points.utm.zone() == (zone, letter)  # (31,'P')
    _, y, zone, letter = utm.from_latlon(
        points.lat(),
        np.median(points.lon()),
        force_zone_number=31,
        force_zone_letter="P",
    )
    np.testing.assert_array_almost_equal(points.x(), x)
    np.testing.assert_array_almost_equal(points.y(), y)

    lon, lat = points.lonlat()
    x, y, zone, letter = utm.from_latlon(lat, lon)
    assert points.utm.zone() == (zone, letter)  # (31,'P')
    xx, yy = points.xy()
    np.testing.assert_array_almost_equal(xx, x)
    np.testing.assert_array_almost_equal(yy, y)

    x, _, zone, letter = utm.from_latlon(
        np.median(points.lat()),
        points.lon(),
        force_zone_number=33,
        force_zone_letter="W",
    )
    assert (zone, letter) == (33, "W")
    _, y, zone, letter = utm.from_latlon(
        points.lat(),
        np.median(points.lon()),
        force_zone_number=33,
        force_zone_letter="W",
    )
    assert (zone, letter) == (33, "W")
    np.testing.assert_array_almost_equal(points.x(utm=(33, "W")), x)
    np.testing.assert_array_almost_equal(points.y(utm=(33, "W")), y)

    points.utm.set((33, "W"))
    lon, lat = points.lonlat()
    x, y, zone, letter = utm.from_latlon(
        lat,
        lon,
        force_zone_number=33,
        force_zone_letter="W",
    )
    xx, yy = points.xy()
    np.testing.assert_array_almost_equal(xx, x)
    np.testing.assert_array_almost_equal(yy, y)


def test_point_cartesian():
    points = PointSkeleton(x=(0, 100, 200, 300), y=(6000000, 6100000, 6400000, 6600000))
    points.utm.set((33, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.x(),
        points.x(utm=(30, "W")),
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.y(),
        points.y(utm=(30, "W")),
    )
    x, y = points.xy()
    x2, y2 = points.xy(utm=(30, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        y,
        y2,
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        x,
        x2,
    )

    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.lon(),
        points.lon(utm=(30, "W")),
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.lat(),
        points.lat(utm=(30, "W")),
    )

    lon, lat = points.xy()
    lon2, lat2 = points.xy(utm=(30, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        lat,
        lat2,
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        lon,
        lon2,
    )


def test_gridded_cartesian():
    points = GriddedSkeleton(
        x=(0, 100, 200, 300), y=(6000000, 6100000, 6400000, 6600000)
    )

    points.utm.set((33, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.x(),
        points.x(utm=(30, "W")),
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.y(),
        points.y(utm=(30, "W")),
    )
    x, y = points.xy()
    x2, y2 = points.xy(utm=(30, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        y,
        y2,
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        x,
        x2,
    )

    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.lon(),
        points.lon(utm=(30, "W")),
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.lat(),
        points.lat(utm=(30, "W")),
    )

    lon, lat = points.xy()
    lon2, lat2 = points.xy(utm=(30, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        lat,
        lat2,
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        lon,
        lon2,
    )


def test_point_spherical():
    points = PointSkeleton(lon=range(1, 6), lat=range(10, 60, 10))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.x(),
        points.x(utm=(30, "W")),
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.y(),
        points.y(utm=(30, "W")),
    )
    x, y = points.xy()
    x2, y2 = points.xy(utm=(30, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        y,
        y2,
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        x,
        x2,
    )
    np.testing.assert_array_almost_equal(points.lon(), points.lon(utm=(30, "W")))
    np.testing.assert_array_almost_equal(points.lat(), points.lat(utm=(30, "W")))

    lon, lat = points.lonlat()
    lon2, lat2 = points.lonlat(utm=(30, "W"))
    np.testing.assert_array_almost_equal(lon, lon2)
    np.testing.assert_array_almost_equal(lat, lat2)


def test_gridded_spherical():
    points = GriddedSkeleton(lon=range(1, 6), lat=range(10, 60, 10))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.x(),
        points.x(utm=(30, "W")),
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        points.y(),
        points.y(utm=(30, "W")),
    )
    x, y = points.xy()
    x2, y2 = points.xy(utm=(30, "W"))
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        y,
        y2,
    )
    np.testing.assert_raises(
        AssertionError,
        np.testing.assert_array_almost_equal,
        x,
        x2,
    )
    np.testing.assert_array_almost_equal(points.lon(), points.lon(utm=(30, "W")))
    np.testing.assert_array_almost_equal(points.lat(), points.lat(utm=(30, "W")))

    lon, lat = points.lonlat()
    lon2, lat2 = points.lonlat(utm=(30, "W"))
    np.testing.assert_array_almost_equal(lon, lon2)
    np.testing.assert_array_almost_equal(lat, lat2)
