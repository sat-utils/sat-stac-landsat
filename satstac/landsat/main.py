import gzip
import logging
import os
import requests
import sys

from datetime import datetime
from dateutil.parser import parse
from satstac import Collection, Item, utils

from .version import __version__


logger = logging.getLogger(__name__)


# pre-collection
# entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon,max_lat,max_lon,download_url
# collection-1
# productId,entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon,max_lat,max_lon,download_url


def add_items(catalog, start_date=None, end_date=None):
    """ Stream records to a collection with a transform function """
    collection = Collection.open(os.path.join(os.path.dirname(__file__), 'landsat-8-l1.json'))
    catalog.add_catalog(collection)

    for i, record in enumerate(records()):
        now = datetime.now()
        dt = record['datetime'].date()
        if (i % 10000) == 0:
            logger.info('%s: %s records scanned' % (datetime.now(), i))
        if start_date is not None and dt < start_date:
            # skip to next if before start_date
            continue
        if end_date is not None and dt > end_date:
            # stop if after end_date
            continue
        try:
            item = transform(record)
        except Exception as err:
            fname = record['url'].replace('index.html', '%s_MTL.txt' % record['id'])
            logger.error('Error getting %s: %s' % (fname, err))
            continue
        try:
            collection.add_item(item, path='${landsat:path}/${landsat:row}/${date}')
            logger.debug('Ingested %s in %s' % (item.id, datetime.now()-now))
        except Exception as err:
            logger.error('Error adding %s: %s' % (item.id, err))
        


def records(collections='all'):
    """ Return generator function for list of scenes """

    filenames = {
        'scene_list.gz': 'https://landsat-pds.s3.amazonaws.com/scene_list.gz',
        'scene_list-c1.gz': 'https://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz'
    }
    if collections == 'pre':
        del filenames['scene_list-c1.gz']
    elif collections == 'c1':
        del filenames['scene_list.gz']

    for fout in filenames:
        filename = filenames[fout]
        if not os.path.exists(fout):
            fout = utils.download_file(filename)
        with gzip.open(fout,'rt') as f:
            header = f.readline()
            for line in f:
                data = line.replace('\n', '').split(',')
                if len(data) == 12:
                    product_id = data[0]
                    data = data[1:]
                    id = product_id
                else:
                    product_id = None
                    id = data[0]
                yield {
                    'id': id,
                    'bbox': [data[7], data[6], data[9], data[8]],
                    'center_lat': (float(data[6]) + float(data[8]))/2,
                    'datetime': parse(data[1]),
                    'properties': {
                        'eo:cloud_cover': data[2],
                        'landsat:product_id': product_id,
                        'landsat:entity_id': data[0],
                        'landsat:processing_level': data[3],
                        'landsat:path': data[4],
                        'landsat:row': data[5]
                    },
                    'url': data[-1]
                }


def transform(data):
    """ Transform Landsat metadata into a STAC item """
    # get metadata
    md = get_metadata(data['url'].replace('index.html', '%s_MTL.txt' % data['id']))

    # geo
    coordinates = [[
        [md['CORNER_UL_LON_PRODUCT'], md['CORNER_UL_LAT_PRODUCT']],
        [md['CORNER_UR_LON_PRODUCT'], md['CORNER_UR_LAT_PRODUCT']],
        [md['CORNER_LR_LON_PRODUCT'], md['CORNER_LR_LON_PRODUCT']],
        [md['CORNER_LL_LON_PRODUCT'], md['CORNER_LL_LAT_PRODUCT']],
        [md['CORNER_UL_LON_PRODUCT'], md['CORNER_UL_LAT_PRODUCT']]
    ]]

    assets = {}

    data['properties'].update({
        'collection': 'landsat-8-l1',
        'datetime': data['datetime'].isoformat(),
        'eo:sun_azimuth': md['SUN_AZIMUTH'],
        'eo:sun_elevation': md['SUN_ELEVATION']
    })

    if 'UTM_ZONE' in md:
        data['properties']['eo:epsg'] = int(('326' if data['center_lat'] > 0 else '327') + md['UTM_ZONE'])

    _item = {
        'type': 'Feature',
        'id': data['id'],
        'bbox': data['bbox'],
        'geometry': {
            'type': 'Polygon',
            'coordinates': coordinates
        },
        'properties': data['properties'],
        'assets': assets
    }
    return Item(_item)


def get_metadata(url):
    """ Convert Landsat MTL file to dictionary of metadata values """
    # Read MTL file remotely
    #r = requests.get(url, stream=True)
    mtl = dict()
    #for line in r.iter_lines():
    for line in read_remote(url):
        meta = line.replace('\"', "").strip().split('=')
        if len(meta) > 1:
            key = meta[0].strip()
            item = meta[1].strip()
            if key != "GROUP" and key != "END_GROUP":
                mtl[key] = item
    return mtl


def read_remote(url):
    """ Return a line iterator for a remote file """
    r = requests.get(url, stream=True)
    for line in r.iter_lines():
        yield line.decode()