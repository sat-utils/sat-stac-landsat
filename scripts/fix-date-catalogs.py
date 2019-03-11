#!/usr/bin/env python

'''
A recent changed caused date sub-catalogs to be created, but there should not be any.
The row catalog should contain `item` links directly to the items, and not have any child
links to date catalogs.

This script takes the items under the date sub-catalogs and adds them to the proper
parent catalog. It does not remove the date catalog links, that's to be another script
'''
import logging
import sys

from satstac import Catalog, Collection

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# quiet loggers
logging.getLogger('urllib3').propagate = False
logging.getLogger('requests').propagate = False

col = Collection.open('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json')

for path in col.children():
    print('Path=%s' % path.id)
    for row in path.children():
        for child in row.children():
            try:
                for item in child.items():
                    col.add_item(item, path='${eo:column}/${eo:row}', filename= '${date}/${id}')
            except Exception as err:
                print('Error in row %s catalog: %s' % (row.id, err))
        #print('rm %s' % child.filename)
        # remove child links
        #links = [l for l in row.data['links'] if l['rel'] != 'child']
