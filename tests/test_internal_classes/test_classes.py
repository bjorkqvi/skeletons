from geo_skeletons.classes import Spectrum1D, Spectrum2D, Wind, Wave, WaveGrid, WindGrid


def test_init_spectra():
    spec1 = Spectrum1D(lon=0, lat=0, time=("2018-01-01", "2018-01-31"), freq=range(10))
    spec2 = Spectrum2D(
        lon=0,
        lat=0,
        time=("2018-01-01", "2018-01-31"),
        dirs=range(0, 350, 10),
        freq=range(10),
    )


def test_wind_and_wave():
    wind = Wind(lon=0, lat=0, time=("2018-01-01", "2018-01-31"))
    wave = Wave(lon=0, lat=0, time=("2018-01-01", "2018-01-31"))


def test_wind_and_wave_grid():
    wind = WindGrid(lon=0, lat=0, time=("2018-01-01", "2018-01-31"))
    wave = WaveGrid(lon=0, lat=0, time=("2018-01-01", "2018-01-31"))
