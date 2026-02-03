from geo_skeletons.classes import WindGrid

def test_empty():
    wind = WindGrid(lon=0, lat=0)
    vars = wind._determine_quicklook_variables(mag=False, dir=False, arrows=False)
    assert vars == {}
    
def test_normal():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=False, dir=False, arrows=False)

    assert vars == {'u':{},'v':{}}

def test_mag():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=True, dir=False, arrows=False)

    assert vars == {'ff':{}}


def test_dir():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=False, dir=True, arrows=False)
    assert vars == {'dd':{'cmap':'twilight'}}


def test_arrows():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=False, dir=False, arrows=True)
    assert vars == {'dd':{'is_arrow':True}}

def test_magdir():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=True, dir=True, arrows=False)
    assert vars == {'dd':{'cmap':'twilight'},'ff':{}}

def test_mag_arrows():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=True, dir=False, arrows=True)
    assert vars == {'ff':{'arrow_data':'dd'}}

def test_mag_dir_arrows():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=True, dir=True, arrows=True)
    assert vars == {'ff':{'arrow_data':'dd'},'dd':{'cmap':'twilight'}}

def test_magdir():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10)
    wind.set_v(5)
    vars = wind._determine_quicklook_variables(mag=False, dir=True, arrows=True)
    assert vars == {'dd':{'cmap':'twilight'},'ff':{'arrow_data':'dd'}}