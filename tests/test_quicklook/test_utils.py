from geo_skeletons.managers.quicklook_manager import determine_levels
import numpy as np

def test_0_to_10():
    levels = determine_levels((0,10))
    np.testing.assert_array_almost_equal(range(0,11), levels)

def test_0_to_20():
    levels = determine_levels((0,20))
    np.testing.assert_array_almost_equal(range(0,21), levels)

def test_0_to_25():
    levels = determine_levels((0,25))
    np.testing.assert_array_almost_equal(range(0,26), levels)

def test_0_to_100():
    levels = determine_levels((0,100))
    np.testing.assert_array_almost_equal(range(0,110,10), levels)

def test_1_to_98():
    levels = determine_levels((1,98))
    np.testing.assert_array_almost_equal(range(0,110,10), levels)

def test_0_to_30():
    levels = determine_levels((0,30))
    np.testing.assert_array_almost_equal(range(0,35,5), levels)

def test_0_to_50():
    levels = determine_levels((0,50))
    np.testing.assert_array_almost_equal(range(0,55,5), levels)

def test_2_to_48():
    levels = determine_levels((2,48))
    np.testing.assert_array_almost_equal(range(0,55,5), levels)

def test_0_to_80():
    levels = determine_levels((0,80))
    np.testing.assert_array_almost_equal(range(0,90,10), levels)

def test_0_to_900():
    levels = determine_levels((0,900))
    np.testing.assert_array_almost_equal(range(0,1000,100), levels)

def test_0_to_4000():
    levels = determine_levels((0,4000))
    np.testing.assert_array_almost_equal(range(0,4500,500), levels)

def test_0_to_4():
    levels = determine_levels((0,4))
    np.testing.assert_array_almost_equal(np.arange(0,4.5,0.5), levels)

def test_0_to_3():
    levels = determine_levels((0,3))
    np.testing.assert_array_almost_equal(np.arange(0,3.5,0.5), levels)

def test_0_to_2():
    levels = determine_levels((0,2))
    np.testing.assert_array_almost_equal(np.arange(0,2.25,0.25), levels)

def test_0_to_1():
    levels = determine_levels((0,1))
    np.testing.assert_array_almost_equal(np.arange(0,1.1,0.1), levels)

def test_0_to_01():
    levels = determine_levels((0,0.1))
    np.testing.assert_array_almost_equal(np.arange(0,0.11,0.01), levels)