from geo_skeletons import GriddedSkeleton


def test_utm_zone_not_None():
    grid = GriddedSkeleton(lon=(0, 1), lat=(10, 11))
    grid.set_spacing(nx=10, ny=10)
