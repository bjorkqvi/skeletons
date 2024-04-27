from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_coord
import numpy as np
import xarray as xr


def compare_das(da1, da2):
    assert da1.name == da2.name
    assert da1.dims == da2.dims
    np.testing.assert_array_almost_equal(da1.data, da2.data)
    assert list(da1.coords) == list(da2.coords)


def test_trivial_point():
    points = PointSkeleton(x=[0, 1, 2], y=[5, 6, 7])
    inds_da = xr.DataArray(
        data=np.array([0, 1, 2]),
        dims=["inds"],
        coords=dict(
            inds=(["inds"], np.array([0, 1, 2])),
        ),
    )
    inds_da.name = "inds"

    daa = points._ds_manager.compile_data_array(np.array([0, 1, 2]), "inds")

    compare_das(points.ds().inds, inds_da)
    compare_das(daa, inds_da)


def test_no_added_coords_point():
    @add_datavar(name="hs")
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=[0, 1, 2], y=[5, 6, 7])
    points.set_hs([7, 8, 9])
    daa = points._ds_manager.compile_data_array(np.array([7, 8, 9]), "hs")

    hs_da = xr.DataArray(
        data=np.array([7, 8, 9]),
        dims=["inds"],
        coords=dict(
            inds=(["inds"], np.array([0, 1, 2])),
        ),
    )
    hs_da.name = "hs"

    # daa  = points._ds_manager.compile_data_array(np.array([0, 1, 2]), "inds")

    compare_das(points.ds().hs, hs_da)
    compare_das(daa, hs_da)


def test_no_added_coords_gridded():
    @add_datavar(name="hs")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=[0, 1, 2, 3], y=[5, 6, 7])
    hs = np.full((3, 4), 999)
    points.set_hs(hs)
    daa = points._ds_manager.compile_data_array(hs, "hs")

    hs_da = xr.DataArray(
        data=hs,
        dims=["y", "x"],
        coords=dict(
            y=(["y"], np.array([5, 6, 7])),
            x=(["x"], np.array([0, 1, 2, 3])),
        ),
    )
    hs_da.name = "hs"

    # daa  = points._ds_manager.compile_data_array(np.array([0, 1, 2]), "inds")

    compare_das(points.ds().hs, hs_da)
    compare_das(daa, hs_da)


def test_added_grid_coord_gridded():
    @add_datavar(name="hs", coord_group="grid")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=[0, 1, 2, 3], y=[5, 6, 7], z=[9, 10])
    hs = np.full((3, 4, 2), 999)
    points.set_hs(hs)
    daa = points._ds_manager.compile_data_array(hs, "hs")

    hs_da = xr.DataArray(
        data=hs,
        dims=["y", "x", "z"],
        coords=dict(
            y=(["y"], np.array([5, 6, 7])),
            x=(["x"], np.array([0, 1, 2, 3])),
            z=(["z"], np.array([9, 10])),
        ),
    )
    hs_da.name = "hs"

    compare_das(points.ds().hs, hs_da)
    compare_das(daa, hs_da)


def test_added_gridpoint_coord_gridded():
    @add_datavar(name="sp", coord_group="spatial")
    @add_datavar(name="gp", coord_group="gridpoint")
    @add_datavar(name="hs", coord_group="grid")
    @add_coord("f", grid_coord=False)
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=[0, 1, 2, 3], y=[5, 6, 7], z=[9, 10], f=[1, 2, 3, 4, 5, 6])
    hs = np.full((3, 4, 2), 999)
    gp = np.full((6,), 888)
    sp = np.full((3, 4), 777)
    points.set_hs(hs)
    points.set_gp(gp)
    points.set_sp(sp)

    daahs = points._ds_manager.compile_data_array(hs, "hs")
    daagp = points._ds_manager.compile_data_array(gp, "gp")
    daasp = points._ds_manager.compile_data_array(sp, "sp")

    hs_da = xr.DataArray(
        data=hs,
        dims=["y", "x", "z"],
        coords=dict(
            y=(["y"], np.array([5, 6, 7])),
            x=(["x"], np.array([0, 1, 2, 3])),
            z=(["z"], np.array([9, 10])),
        ),
    )
    hs_da.name = "hs"

    gp_da = xr.DataArray(
        data=gp,
        dims=["f"],
        coords=dict(
            f=(["f"], np.array([1, 2, 3, 4, 5, 6])),
        ),
    )

    gp_da.name = "gp"

    sp_da = xr.DataArray(
        data=sp,
        dims=["y", "x"],
        coords=dict(
            y=(["y"], np.array([5, 6, 7])),
            x=(["x"], np.array([0, 1, 2, 3])),
        ),
    )
    sp_da.name = "sp"

    compare_das(points.ds().hs, hs_da)
    compare_das(daahs, hs_da)

    compare_das(points.ds().gp, gp_da)
    compare_das(daagp, gp_da)

    compare_das(points.ds().sp, sp_da)
    compare_das(daasp, sp_da)


# def test_add_datavar():
#     points = PointSkeleton(x=0, y=4)
#     points.add_datavar("hs")
#     assert "hs" in points.core.data_vars()
#     assert "hs" not in list(points.ds().keys())
#     points.set_hs()
#     assert "hs" in list(points.ds().keys())


# def test_add_datavar_on_top():
#     @add_datavar(name="hs")
#     @add_coord(name="z")
#     class Expanded(PointSkeleton):
#         pass
