import numpy as np
import dask.array as da
import pytest
from geo_skeletons.dir_conversions import (
    convert,
    convert_from_math_dir,
    convert_to_math_dir,
)


def data_is_dask(data) -> bool:
    """Checks if a data array is a dask array"""
    return hasattr(data, "chunks") and data.chunks is not None


@pytest.fixture
def to_data():
    return np.array([[0, 45, 90], [180, 315, 270]])


@pytest.fixture
def from_data():
    return np.array([[0 + 180, 45 + 180, 90 + 180], [180 - 180, 315 - 180, 270 - 180]])


@pytest.fixture
def math_data():
    return np.array([[90, 45, 0], [270 - 360, 135, 180]]) * np.pi / 180


def test_math_data_pi_to_minus_pi():
    rng = np.random.default_rng()
    data = rng.random((100, 100, 100)) * 360
    mathdata = convert_to_math_dir(data, dir_type="to")
    assert np.min(mathdata) >= -np.pi
    assert np.max(mathdata) <= np.pi
    mathdata = convert_to_math_dir(data, dir_type="from")
    assert np.min(mathdata) >= -np.pi
    assert np.max(mathdata) <= np.pi


def test_convert_to_math_dir_numpy(to_data, from_data, math_data):
    """Math dir gives out -np.pi to np.pi"""
    mathdata = convert_to_math_dir(to_data, dir_type="to")
    np.testing.assert_array_almost_equal(math_data, mathdata)

    math_data = convert_to_math_dir(from_data, dir_type="from")
    np.testing.assert_array_almost_equal(math_data, mathdata)


def test_convert_from_math_dir_numpy(to_data, from_data, math_data):
    """Math dir gives out -np.pi to np.pi"""
    todata = convert_from_math_dir(math_data, dir_type="to")
    np.testing.assert_array_almost_equal(todata, to_data)

    fromdata = convert_from_math_dir(math_data, dir_type="from")
    np.testing.assert_array_almost_equal(fromdata, from_data)


def test_convert_to_math_dir_dask(to_data, from_data, math_data):
    """Math dir gives out -np.pi to np.pi"""
    mathdata = convert_to_math_dir(da.from_array(to_data), dir_type="to")
    assert data_is_dask(mathdata)
    np.testing.assert_array_almost_equal(math_data, mathdata.compute())

    math_data = convert_to_math_dir(from_data, dir_type="from")
    assert data_is_dask(mathdata)
    np.testing.assert_array_almost_equal(math_data, mathdata.compute())


def test_convert_from_math_dir_dask(to_data, from_data, math_data):
    """Math dir gives out -np.pi to np.pi"""
    todata = convert_from_math_dir(da.from_array(math_data), dir_type="to")
    assert data_is_dask(todata)
    np.testing.assert_array_almost_equal(to_data, todata.compute())

    fromdata = convert_from_math_dir(da.from_array(math_data), dir_type="from")
    assert data_is_dask(fromdata)
    np.testing.assert_array_almost_equal(from_data, fromdata.compute())


def test_convert_numpy(to_data, from_data, math_data):
    np.testing.assert_array_almost_equal(
        convert(to_data, in_type="to", out_type="from"), from_data
    )
    np.testing.assert_array_almost_equal(
        convert(to_data, in_type="to", out_type="math"), math_data
    )
    np.testing.assert_array_almost_equal(
        convert(to_data, in_type="to", out_type="to"), to_data
    )

    np.testing.assert_array_almost_equal(
        convert(from_data, in_type="from", out_type="from"), from_data
    )
    np.testing.assert_array_almost_equal(
        convert(from_data, in_type="from", out_type="math"), math_data
    )
    np.testing.assert_array_almost_equal(
        convert(from_data, in_type="from", out_type="to"), to_data
    )

    np.testing.assert_array_almost_equal(
        convert(math_data, in_type="math", out_type="from"), from_data
    )
    np.testing.assert_array_almost_equal(
        convert(math_data, in_type="math", out_type="math"), math_data
    )
    np.testing.assert_array_almost_equal(
        convert(math_data, in_type="math", out_type="to"), to_data
    )


def test_convert_dask(to_data, from_data, math_data):
    np.testing.assert_array_almost_equal(
        convert(da.from_array(to_data).compute(), in_type="to", out_type="from"),
        from_data,
    )
    np.testing.assert_array_almost_equal(
        convert(da.from_array(to_data).compute(), in_type="to", out_type="math"),
        math_data,
    )
    np.testing.assert_array_almost_equal(
        convert(da.from_array(to_data).compute(), in_type="to", out_type="to"),
        to_data,
    )

    np.testing.assert_array_almost_equal(
        convert(da.from_array(from_data).compute(), in_type="from", out_type="from"),
        from_data,
    )
    np.testing.assert_array_almost_equal(
        convert(da.from_array(from_data).compute(), in_type="from", out_type="math"),
        math_data,
    )
    np.testing.assert_array_almost_equal(
        convert(da.from_array(from_data).compute(), in_type="from", out_type="to"),
        to_data,
    )

    np.testing.assert_array_almost_equal(
        convert(da.from_array(math_data).compute(), in_type="math", out_type="from"),
        from_data,
    )
    np.testing.assert_array_almost_equal(
        convert(da.from_array(math_data).compute(), in_type="math", out_type="math"),
        math_data,
    )
    np.testing.assert_array_almost_equal(
        convert(da.from_array(math_data).compute(), in_type="math", out_type="to"),
        to_data,
    )
