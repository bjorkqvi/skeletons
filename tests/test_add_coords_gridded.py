from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_direction, add_frequency, add_time
import numpy as np
import pandas as pd


def test_1d_spectrum():
    @add_frequency(grid_coord=False)
    class Spectrum(GriddedSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), freq=freqs)
    np.testing.assert_array_almost_equal(spec.freq(), freqs)
    np.testing.assert_array_almost_equal(spec.freq(angular=True), freqs * 2 * np.pi)


def test_1d_spectrum_new_name():
    @add_frequency(grid_coord=False, name="f")
    class Spectrum(GriddedSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), f=freqs)
    np.testing.assert_array_almost_equal(spec.f(), freqs)
    np.testing.assert_array_almost_equal(spec.f(angular=True), freqs * 2 * np.pi)


def test_2d_spectrum():
    @add_frequency(grid_coord=False)
    @add_direction(grid_coord=False)
    class Spectrum(GriddedSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    dirs = np.linspace(0, 355, 36)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), freq=freqs, dirs=dirs)
    np.testing.assert_array_almost_equal(spec.freq(), freqs)
    np.testing.assert_array_almost_equal(spec.dirs(), dirs)
    np.testing.assert_array_almost_equal(spec.freq(angular=True), freqs * 2 * np.pi)
    np.testing.assert_array_almost_equal(spec.dirs(angular=True), dirs * np.pi / 180)


def test_2d_spectrum_new_names():
    @add_frequency(grid_coord=False, name="f")
    @add_direction(grid_coord=False, name="D")
    class Spectrum(GriddedSkeleton):
        pass

    freqs = np.linspace(0.0, 1.0, 11)
    dirs = np.linspace(0, 355, 36)
    spec = Spectrum(x=(0.0, 1.0), y=(10.0, 20.0), f=freqs, D=dirs)
    np.testing.assert_array_almost_equal(spec.f(), freqs)
    np.testing.assert_array_almost_equal(spec.D(), dirs)
    np.testing.assert_array_almost_equal(spec.f(angular=True), freqs * 2 * np.pi)
    np.testing.assert_array_almost_equal(spec.D(angular=True), dirs * np.pi / 180)


def test_add_z_coord():
    @add_coord(grid_coord=True, name="z")
    class a3DGrid(GriddedSkeleton):
        pass

    grid = a3DGrid(x=(0.0, 1.0), y=(10.0, 20.0), z=(0.0, 10.0))
    grid.set_spacing(nx=5, ny=6)
    grid.set_z_spacing(nx=11)
    np.testing.assert_array_almost_equal(grid.z(), np.linspace(0.0, 10.0, 11))
    grid.set_z_spacing(dx=1)
    np.testing.assert_array_almost_equal(grid.z(), np.linspace(0.0, 10.0, 11))

    assert grid.size() == (6, 5, 11)
    assert grid.size(coord_group="all") == (6, 5, 11)
    assert grid.size(coord_group="spatial") == (6, 5)
    assert grid.size(coord_group="grid") == (6, 5, 11)
    assert grid.size(coord_group="gridpoint") == ()


def test_add_time_coord():
    @add_time(grid_coord=False)
    class TimeSeries(GriddedSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1h")
    ts = TimeSeries(x=(0.0, 1.0), y=(10.0, 20.0), time=times)
    for n, t in enumerate(times):
        assert t == ts.time()[n]

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


def test_add_z_and_time_coord():
    @add_time(grid_coord=False)
    @add_coord(grid_coord=True, name="z")
    class TimeSeries(GriddedSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1h")
    ts = TimeSeries(x=(0.0, 1.0), y=(10.0, 20.0), time=times, z=(10, 20))
    ts.set_spacing(nx=5, ny=6)
    ts.set_z_spacing(nx=11)

    assert ts.size() == (len(times), 6, 5, 11)
    assert ts.size(coord_group="all") == (len(times), 6, 5, 11)
    assert ts.size(coord_group="spatial") == (6, 5)
    assert ts.size(coord_group="grid") == (6, 5, 11)
    assert ts.size(coord_group="gridpoint") == (len(times),)

    for n, t in enumerate(times):
        assert t == ts.time()[n]

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
