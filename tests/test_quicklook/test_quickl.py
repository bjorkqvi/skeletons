from geo_skeletons.classes import WindGrid, Wind
import matplotlib.pyplot as plt

def test_gridded_lonlat():
    data = WindGrid(lon=(10, 20), lat=(50,60))
    data.set_spacing(nx=20, ny=10)
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
def test_gridded_lonlat_with_time():
    data = WindGrid.add_time()(lon=(10, 20), lat=(50,60), time=('2020-01-01 00:00','2020-01-01 10:00'))
    data.set_spacing(nx=20, ny=10)
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
def test_gridded_xy_cart():
    data = WindGrid(x=(440892.10517494264, 559107.8948250574), y=(6429147.6117879255, 6651832.7361561125))
    data.proj.set((34, 'V'))
    data.set_spacing(nx=20, ny=10)
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
def test_gridded_xy_rot():
   
    data = WindGrid(x=(26.21,27.37), y= (4.33,5.29))
    data.proj.set('+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs')
    data.set_spacing(nx=20, ny=10)
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
    

def test_point_lonlat():
    data = Wind(lon=(10, 20), lat=(50,60))
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
    

def test_gridded_xy_cart():
    data = Wind(x=(440892.10517494264, 559107.8948250574), y=(6429147.6117879255, 6651832.7361561125))
    data.proj.set((34, 'V'))
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
def test_gridded_xy_rot():
   
    data = Wind(x=(26.21,27.37), y= (4.33,5.29))
    data.proj.set('+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs')
    data.set_ff(10)
    data.set_dd(90)

    for proj in [None, 'lonlat','xy']:
        for contour in [True, False]:
            for mag in [True, False]:
                for dir in [True, False]:
                    for arrows in [True, False]:
                        for rotated in [True, False]:
                            data.quicklook(show=False, proj=proj, contour=contour, mag=mag, dir=dir, arrows=arrows, rotated=rotated)
                            plt.close()
    