from geo_skeletons import PointSkeleton, GriddedSkeleton

import pytest

def test_point():
    with pytest.raises(TypeError):
        data = PointSkeleton(x=2, y=6, c=5, crs=(33,'W'))