from geo_skeletons import PointSkeleton
import numpy as np
import geo_parameters as gp

def test_add_list_of_datavars_str():
    list_of_vars = ['hs', 'tp']
    points = PointSkeleton.add_datavar(list_of_vars)(x=0, y=4)

    for var in list_of_vars:
        assert var in points.core.data_vars()      
        assert var not in list(points.ds().keys())
        points.set(var)
        assert var in list(points.ds().keys())
        assert points.core.coord_group(var) == 'all'
        np.testing.assert_almost_equal(points.core.default_value(var), 0.0)

def test_add_list_of_datavars_gp():
    list_of_vars = [gp.wave.Hs, gp.wave.Tp]
    points = PointSkeleton.add_datavar(list_of_vars)(x=0, y=4)

    for var in list_of_vars:
        assert var.name in points.core.data_vars()      
        assert var.name not in list(points.ds().keys())
        points.set(var)
        assert var.name in list(points.ds().keys())
        assert points.core.coord_group(var.name) == 'all'
        np.testing.assert_almost_equal(points.core.default_value(var.name), 0.0)

def test_add_list_of_datavars_str_different_coord_groups():
    list_of_vars = ['hs', 'tp']
    coord_groups = ['all', 'grid']
    points = PointSkeleton.add_datavar(list_of_vars, coord_group=coord_groups)(x=0, y=4)

    for var, cg in zip(list_of_vars, coord_groups):
        assert var in points.core.data_vars()      
        assert var not in list(points.ds().keys())
        points.set(var)
        assert var in list(points.ds().keys())
        assert points.core.coord_group(var) == cg
        np.testing.assert_almost_equal(points.core.default_value(var), 0.0)

def test_add_list_of_datavars_gp_different_coord_groups():
    list_of_vars = [gp.wave.Hs, gp.wave.Tp]
    coord_groups = ['spatial', 'gridpoint']
    points = PointSkeleton.add_datavar(list_of_vars, coord_group=coord_groups)(x=0, y=4)
    for var, cg in zip(list_of_vars, coord_groups):
        assert var.name in points.core.data_vars()      
        assert var.name not in list(points.ds().keys())
        points.set(var)
        assert var.name in list(points.ds().keys())
        assert points.core.coord_group(var.name) == cg
        np.testing.assert_almost_equal(points.core.default_value(var.name), 0.0)


def test_add_list_of_datavars_str_different_coord_groups_and_default_values():
    list_of_vars = ['hs', 'tp']
    coord_groups = ['all', 'grid']
    default_values = [1.0, np.nan]
    points = PointSkeleton.add_datavar(list_of_vars, coord_group=coord_groups, default_value=default_values)(x=0, y=4)

    for var, cg, dv in zip(list_of_vars, coord_groups, default_values):
        assert var in points.core.data_vars()      
        assert var not in list(points.ds().keys())
        points.set(var)
        assert var in list(points.ds().keys())
        assert points.core.coord_group(var) == cg
        np.testing.assert_almost_equal(points.core.default_value(var), dv)

def test_add_list_of_datavars_gp_different_coord_groups_and_default_values():
    list_of_vars = [gp.wave.Hs, gp.wave.Tp]
    coord_groups = ['spatial', 'gridpoint']
    default_values = [1.0, np.nan]
    points = PointSkeleton.add_datavar(list_of_vars, coord_group=coord_groups, default_value=default_values)(x=0, y=4)
    for var, cg,dv in zip(list_of_vars, coord_groups, default_values):
        assert var.name in points.core.data_vars()      
        assert var.name not in list(points.ds().keys())
        points.set(var)
        assert var.name in list(points.ds().keys())
        assert points.core.coord_group(var.name) == cg
        np.testing.assert_almost_equal(points.core.default_value(var.name), dv)