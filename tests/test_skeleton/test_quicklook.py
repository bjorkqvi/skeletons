from geo_skeletons import PointSkeleton, GriddedSkeleton
def test_quicklook():
    data=[]
    clss = [PointSkeleton, GriddedSkeleton]
    for cls in clss:
        data.append(cls(x=(10,29), y=(30,40)))
        data.append(cls.add_time()(x=(10,29), y=(30,40), time=('2020-01-01 00:00', '2020-01-01 04:00')))
        junk = cls.add_datavar('hs')(x=(10,29), y=(30,40))
        junk.set_hs(3)
        data.append(junk)
        junk = cls.add_time().add_datavar('hs')(x=(10,29), y=(30,40), time=('2020-01-01 00:00', '2020-01-01 04:00'))
        junk.set_hs(2)
        data.append(junk)

    for da in data:
        print(da)
        da.quicklook(show=False)