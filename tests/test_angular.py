from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import numpy as np
import pytest
import geo_parameters as gp


def test_angular_str():
    @add_datavar("stokes_dir", default_value=0, dir_type="from")
    @add_datavar("stokes", default_value=0.1)
    @add_magnitude(name="wind", x="u", y="v", direction="wdir", dir_type="from")
    @add_datavar("v", default_value=-1)
    @add_datavar("u", default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))

    np.testing.assert_almost_equal(np.mean(points.u()), 1)
    np.testing.assert_almost_equal(np.mean(points.v()), -1)
    np.testing.assert_almost_equal(np.mean(points.wind()), 2**0.5)

    np.testing.assert_almost_equal(np.mean(points.wdir()), 315)
    np.testing.assert_almost_equal(np.mean(points.stokes()), 0.1)
    np.testing.assert_almost_equal(np.mean(points.stokes_dir()), 0)

    assert points.u(strict=True) is None
    assert points.v(strict=True) is None
    assert points.wind(strict=True) is None
    assert points.wdir(strict=True) is None
    assert points.stokes(strict=True) is None
    assert points.stokes_dir(strict=True) is None

    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, dask=False)), 135 + 180
    )
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, dir_type="math", dask=False)), -np.pi / 4
    )

    points.stokes_dir(dir_type="from")
    np.testing.assert_almost_equal(
        np.median(points.stokes_dir(empty=True, dask=False)), 0
    )

    np.testing.assert_almost_equal(
        np.median(points.stokes_dir(empty=True, dir_type="math", dask=False)),
        -np.pi / 2,
    )
    with pytest.raises(ValueError):
        points.stokes(dir_type="math")


def test_angular_gp():
    @add_datavar(gp.wave.StokesDir, default_value=0)
    @add_datavar(gp.wave.Stokes, default_value=0.1)
    @add_magnitude(gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDir("wdir"))
    @add_datavar(gp.wind.YWind("v"), default_value=-1)
    @add_datavar(gp.wind.YWind("u"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))

    np.testing.assert_almost_equal(np.mean(points.u()), 1)
    np.testing.assert_almost_equal(np.mean(points.v()), -1)
    np.testing.assert_almost_equal(np.mean(points.wind()), 2**0.5)
    np.testing.assert_almost_equal(np.mean(points.wdir()), 315)
    np.testing.assert_almost_equal(np.mean(points.us()), 0.1)
    np.testing.assert_almost_equal(np.mean(points.us_dir()), 0)

    assert points.u(strict=True) is None
    assert points.v(strict=True) is None
    assert points.wind(strict=True) is None
    assert points.wdir(strict=True) is None
    assert points.us(strict=True) is None
    assert points.us_dir(strict=True) is None
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, dask=False)), 135 + 180
    )
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, dir_type="math", dask=False)), -np.pi / 4
    )
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, dir_type="to", dask=False)), 135
    )
    np.testing.assert_almost_equal(np.median(points.us_dir(empty=True, dask=False)), 0)

    points.us_dir(empty=True, dir_type="math")
    np.testing.assert_almost_equal(
        np.median(points.us_dir(empty=True, dir_type="math", dask=False)), np.pi / 2
    )

    points.set_us_dir(180)
    np.testing.assert_almost_equal(
        np.median(points.us_dir(dir_type="math")), -np.pi / 2
    )

    with pytest.raises(ValueError):
        points.us(dir_type="math")


def test_angular_gp_flip_dir():
    @add_datavar(gp.wave.StokesDirFrom, default_value=0)
    @add_datavar(gp.wave.Stokes, default_value=0.1)
    @add_magnitude(gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDirTo("wdir"))
    @add_datavar(gp.wind.YWind("v"), default_value=-1)
    @add_datavar(gp.wind.YWind("u"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7), chunks="auto")
    points.dask.deactivate()

    np.testing.assert_almost_equal(np.mean(points.u()), 1)
    np.testing.assert_almost_equal(np.mean(points.v()), -1)
    np.testing.assert_almost_equal(np.mean(points.wind()), 2**0.5)
    np.testing.assert_almost_equal(np.mean(points.wdir()), 135)
    np.testing.assert_almost_equal(np.mean(points.us()), 0.1)
    np.testing.assert_almost_equal(np.mean(points.us_dir()), 0)

    assert points.u(strict=True) is None
    assert points.v(strict=True) is None
    assert points.wind(strict=True) is None
    assert points.wdir(strict=True) is None
    assert points.us(strict=True) is None
    assert points.us_dir(strict=True) is None

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True, dask=False)), 135)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, dir_type="math", dask=False)), -np.pi / 4
    )

    np.testing.assert_almost_equal(np.median(points.us_dir(empty=True, dask=False)), 0)
    np.testing.assert_almost_equal(
        np.median(points.us_dir(empty=True, dir_type="from", dask=False)), 0
    )
    np.testing.assert_almost_equal(
        np.median(points.us_dir(empty=True, dir_type="to", dask=False)), 180
    )
    np.testing.assert_almost_equal(
        np.median(points.us_dir(empty=True, dir_type="math", dask=False)), -np.pi / 2
    )
    with pytest.raises(ValueError):
        points.us(dir_type="from")
