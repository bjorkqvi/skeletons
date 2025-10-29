from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.decorators import add_coord, add_direction, add_frequency, add_time
import numpy as np
import pandas as pd


def test_1d_spectrum():
    @add_frequency(grid_coord=False)
    class Spectrum(PointSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), freq=freqs)
    np.testing.assert_array_almost_equal(spec.freq(), freqs)
    np.testing.assert_array_almost_equal(spec.freq(angular=True), freqs * 2 * np.pi)

    assert spec.size() == (2, len(freqs))
    assert spec.size(coord_group="all") == (2, len(freqs))
    assert spec.size(coord_group="spatial") == (2,)
    assert spec.size(coord_group="grid") == (2,)
    assert spec.size(coord_group="gridpoint") == (len(freqs),)


def test_1d_spectrum_point():
    @add_frequency(grid_coord=False, name="f")
    class Spectrum(PointSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), f=freqs)
    np.testing.assert_array_almost_equal(spec.f(), freqs)
    np.testing.assert_array_almost_equal(spec.f(angular=True), freqs * 2 * np.pi)

    assert spec.size() == (2, len(freqs))
    assert spec.size(coord_group="all") == (2, len(freqs))
    assert spec.size(coord_group="spatial") == (2,)
    assert spec.size(coord_group="grid") == (2,)
    assert spec.size(coord_group="gridpoint") == (len(freqs),)


def test_2d_spectrum():
    @add_direction(grid_coord=False)
    @add_frequency(grid_coord=False)
    class Spectrum(PointSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    dirs = np.linspace(0, 355, 36)
    spec = Spectrum(dirs=dirs, x=(0.0, 1.0), y=(10.0, 20.0), freq=freqs)

    np.testing.assert_array_almost_equal(spec.freq(), freqs)
    np.testing.assert_array_almost_equal(spec.dirs(), dirs)
    np.testing.assert_array_almost_equal(spec.freq(angular=True), freqs * 2 * np.pi)
    np.testing.assert_array_almost_equal(spec.dirs(angular=True), dirs * np.pi / 180)
    np.testing.assert_almost_equal(spec.dd(), 10)
    np.testing.assert_almost_equal(spec.dd(angular=True), 10 * np.pi / 180)
    np.testing.assert_almost_equal(spec.df(angular=True), 0.1 * 2 * np.pi)

    assert spec.size() == (2, len(freqs), len(dirs))
    assert spec.size(coord_group="all") == (2, len(freqs), len(dirs))
    assert spec.size(coord_group="spatial") == (2,)
    assert spec.size(coord_group="grid") == (2,)
    assert spec.size(coord_group="gridpoint") == (len(freqs), len(dirs))


def test_2d_spectrum_new_names():
    @add_frequency(grid_coord=False, name="f")
    @add_direction(grid_coord=False, name="D")
    class Spectrum(PointSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    dirs = np.linspace(0, 355, 36)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), f=freqs, D=dirs)
    np.testing.assert_array_almost_equal(spec.f(), freqs)
    np.testing.assert_array_almost_equal(spec.D(), dirs)
    np.testing.assert_array_almost_equal(spec.f(angular=True), freqs * 2 * np.pi)
    np.testing.assert_array_almost_equal(spec.D(angular=True), dirs * np.pi / 180)

    assert spec.size() == (2, len(dirs), len(freqs))
    assert spec.size(coord_group="all") == (2, len(dirs), len(freqs))
    assert spec.size(coord_group="spatial") == (2,)
    assert spec.size(coord_group="grid") == (2,)
    assert spec.size(coord_group="gridpoint") == (len(dirs), len(freqs))


def test_3d_spectrum():
    @add_coord(grid_coord=False, name="ky")
    @add_coord(grid_coord=False, name="kx")
    @add_frequency(grid_coord=False)
    class Spectrum(PointSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    k = np.linspace(0.0, 10.0, 21)

    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), freq=freqs, kx=k, ky=k)

    np.testing.assert_array_almost_equal(spec.freq(), freqs)
    np.testing.assert_array_almost_equal(spec.kx(), k)
    np.testing.assert_array_almost_equal(spec.ky(), k)
    np.testing.assert_array_almost_equal(spec.freq(angular=True), freqs * 2 * np.pi)

    assert spec.size() == (2, len(freqs), len(k), len(k))
    assert spec.size(coord_group="all") == (2, len(freqs), len(k), len(k))
    assert spec.size(coord_group="spatial") == (2,)
    assert spec.size(coord_group="grid") == (2,)
    assert spec.size(coord_group="gridpoint") == (len(freqs), len(k), len(k))


def test_timeseries():
    @add_time(grid_coord=False)
    class TimeSeries(PointSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1h")
    ts = TimeSeries(x=(0.0, 1.0), y=(10.0, 20.0), time=times)

    assert ts.size() == (len(times), 2)
    assert ts.size(coord_group="all") == (len(times), 2)
    assert ts.size(coord_group="spatial") == (2,)
    assert ts.size(coord_group="grid") == (2,)
    assert ts.size(coord_group="gridpoint") == (len(times),)

    for n, t in enumerate(times):
        assert t == ts.time()[n]
        assert t == ts.hours()[n]
    days = ts.days(datetime=True)[0:-1]
    reconstructed_days = [
        pd.to_datetime(f"2018-01-{dd:02.0f}") for dd in np.linspace(1, 31, 31)
    ]
    for n, d in enumerate(days):
        assert d == reconstructed_days[n]

    months = ts.months(datetime=True)
    reconstructed_months = pd.to_datetime(["2018-01", "2018-02"])
    for n, m in enumerate(months):
        assert m == reconstructed_months[n]

    assert ts.years(datetime=True)[0] == pd.to_datetime("2018")

    assert ts.years(datetime=False)[0] == "2018"
    assert ts.months(datetime=False) == ["2018-01", "2018-02"]
    assert ts.days(datetime=False)[0:-1] == [
        f"2018-01-{dd:02.0f}" for dd in np.linspace(1, 31, 31)
    ]
    assert ts.days(datetime=False)[-1] == "2018-02-01"
