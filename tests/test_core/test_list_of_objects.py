from geo_skeletons import PointSkeleton, GriddedSkeleton

def test_masks():
    PointSpectrum = PointSkeleton.add_time().add_frequency().add_direction().add_mask('land', coord_group='spatial').add_mask('swell', coord_group='gridpoint').add_mask('missing', coord_group='grid')

    assert set(PointSpectrum.core.masks('all')) == {'land_mask', 'swell_mask', 'missing_mask'}
    assert set(PointSpectrum.core.masks('spatial')) == {'land_mask'}
    assert set(PointSpectrum.core.masks('grid')) == {'land_mask','missing_mask'}
    assert set(PointSpectrum.core.masks('gridpoint')) == {'swell_mask'}


def test_mask_points():
    PointSpectrum = PointSkeleton.add_time().add_frequency().add_direction().add_mask('land', coord_group='spatial').add_mask('swell', coord_group='gridpoint').add_mask('missing', coord_group='grid')

    assert set(PointSpectrum.core.mask_points('all')) == {'land_points', 'swell_points', 'missing_points'}
    assert set(PointSpectrum.core.mask_points('spatial')) == {'land_points'}
    assert set(PointSpectrum.core.mask_points('grid')) == {'land_points', 'missing_points'}
    assert set(PointSpectrum.core.mask_points('gridpoint')) == {'swell_points'}


def test_data_vars():
    Wave = GriddedSkeleton.add_time().add_datavar('hs', coord_group='grid').add_datavar('tp').add_datavar('depth', coord_group='spatial')
    assert set(Wave.core.data_vars()) == {'hs','tp'}
    assert set(Wave.core.data_vars('spatial')) == {'depth'}
    assert set(Wave.core.data_vars('grid')) == {'hs', 'depth'}
    assert set(Wave.core.data_vars('gridpoint')) == set({})

def test_data_vars_point():
    Wave =PointSkeleton.add_time().add_datavar('hs', coord_group='grid').add_datavar('tp').add_datavar('depth', coord_group='spatial')
    assert set(Wave.core.data_vars()) == {'hs','tp'}
    assert set(Wave.core.data_vars('spatial')) == {'depth','x','y'}
    assert set(Wave.core.data_vars('grid')) == {'hs', 'depth','x','y'}
    assert set(Wave.core.data_vars('gridpoint')) == set({})