import os
import unittest

from datetime import datetime as dt

from satstac import Catalog, landsat

testpath = os.path.dirname(__file__)


class Test(unittest.TestCase):
    """ Test main module """

    def test_main(self):
        """ Run main function """
        # create test catalog
        fname = os.path.join(testpath, 'test_main', 'catalog.json')
        cat = Catalog.create(id='test').save_as(fname)
        assert(os.path.exists(fname))
        #fout = landsat.main(landsat, start_date=dt(2013, 10, 1).date())

    def test_records(self):
        for r in landsat.records():
            assert(str(r['datetime'].date()) == '2015-01-02')
            break

    def test_c1_records(self):
        for r in landsat.records(collections='c1'):
            assert(r['id'] == 'LC08_L1TP_149039_20170411_20170415_01_T1')
            break

    def test_transform(self):
        for r in landsat.records():
            item = landsat.transform(r)
            assert(item.id == 'LC80101172015002')
            assert(str(item.date) == '2015-01-02')
            break

    def test_c1_transform(self):
        for r in landsat.records(collections='c1'):
            item = landsat.transform(r)
            assert(str(item.date) == '2017-04-11')
            break

    def test_get_metadata(self):
        """ Read Landsat MTL file """
        id = 'LC08_L1GT_086240_20180827_20180827_01_RT'
        url = 'https://landsat-pds.s3.amazonaws.com/c1/L8/086/240/%s/%s_MTL.txt' % (id, id)
        md = landsat.get_metadata(url)
        assert(md['LANDSAT_PRODUCT_ID'] == id)