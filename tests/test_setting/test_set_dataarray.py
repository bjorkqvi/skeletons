from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar
import pytest
from geo_skeletons.errors import DataWrongDimensionError


def test_set_dataarray():
    @add_datavar("hs")
    @add_coord("test")
    class Expanded1(GriddedSkeleton):
        pass

    grid = Expanded1(x=(1, 2, 3), y=(4, 5, 6, 7, 8), test=(9, 10), chunks=None)
    grid.set_hs(1)
    data = grid.hs(data_array=True) * 2

    grid.set_hs(data)
    assert (grid.hs() == data.data).all()
    assert (grid.hs(data_array=True) == data).all()


def test_set_dataarray_wrong_variable_name():
    @add_datavar("hs")
    @add_coord("test")
    class Expanded1(GriddedSkeleton):
        pass

    @add_datavar("hs")
    @add_coord("test2")
    class Expanded2(GriddedSkeleton):
        pass

    grid = Expanded1(x=(1, 2, 3), y=(4, 5, 6, 7, 8), test=(9, 10), chunks=None)
    grid2 = Expanded2(x=(1, 2, 3), y=(4, 5, 6, 7, 8), test2=(9, 10), chunks=None)
    grid.set_hs(1)
    data = grid.hs(data_array=True) * 2

    with pytest.raises(DataWrongDimensionError):
        grid2.set_hs(data)


def test_set_dataarray_wrong_trivial_variable_name():
    @add_datavar("hs")
    @add_coord("test")
    class Expanded1(GriddedSkeleton):
        pass

    @add_datavar("hs")
    @add_coord("test2")
    class Expanded2(GriddedSkeleton):
        pass

    grid = Expanded1(x=(1, 2, 3), y=(4, 5, 6, 7, 8), test=(9), chunks=None)
    grid2 = Expanded2(x=(1, 2, 3), y=(4, 5, 6, 7, 8), test2=(9), chunks=None)
    grid.set_hs(1)
    data = grid.hs(data_array=True) * 2

    # test/test2 is named wrong, but it is ignored since it is of length one
    grid2.set_hs(data)
    assert (grid2.hs() == data.data).all()
    assert (grid2.hs(data_array=True) == data).all()


def test_set_dataarray_wrong_variable_name_transpose():
    @add_datavar("hs")
    @add_coord("another_trivial")
    @add_coord("another")
    @add_coord("test")
    @add_coord("trivial")
    class Expanded1(GriddedSkeleton):
        pass

    @add_datavar("hs")
    @add_coord("another_trivial2")
    @add_coord("test")
    @add_coord("trivial2")
    @add_coord("another")
    class Expanded2(GriddedSkeleton):
        pass

    grid = Expanded1(
        x=(1, 2, 3),
        y=(4, 5, 6, 7, 8),
        trivial=0,
        test=(9, 10),
        another=(1, 2, 3, 4),
        another_trivial=1,
        chunks=None,
    )
    grid2 = Expanded2(
        x=(1, 2, 3),
        y=(4, 5, 6, 7, 8),
        another=(2, 3, 4, 5),
        trivial2=0,
        test=(10, 11),
        another_trivial2=5,
        chunks=None,
    )
    # Squeezed data
    grid.set_hs(1)
    data = grid.hs(data_array=True) * 2
    data.data[:, :, :, 0] = 5

    # Ignores the trivial dimensions, but identifies non-trivial matching dimensions and performs a reshape
    grid2.set_hs(data)
    # Check that the transpose has been done right
    assert (grid2.hs()[:, :, 0, :] == data.data[:, :, :, 0]).all()
    assert (grid2.hs(data_array=True)[:, :, 0, :] == data[:, :, :, 0]).all()

    # Not squeezed data
    grid.set_hs(1)
    data = grid.hs(data_array=True, squeeze=False) * 2
    data.data[:, :, :, 0, 1, :] = 5

    # Ignores the trivial dimensions, but identifies non-trivial matching dimensions and performs a reshape
    grid2.set_hs(data)
    # Check that the transpose has been done right

    assert (
        grid2.hs(squeeze=False)[:, :, 1, :, 0, :] == data.data[:, :, :, 0, 1, :]
    ).all()
    assert (
        grid2.hs(data_array=True, squeeze=False)[:, :, 1, :, 0, :]
        == data[:, :, :, 0, 1, :]
    ).all()
