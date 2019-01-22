#!/usr/bin/env python

'''
A recent changed caused date sub-catalogs to be created, but there should not be any.
The row catalog should contain `item` links directly to the items, and not have any child
links to date catalogs.

This script fins date catalogs and removes the links to them
'''
import logging
import sys

import os.path as op
from satstac import Catalog, Collection

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# quiet loggers
logging.getLogger('urllib3').propagate = False
logging.getLogger('requests').propagate = False

col = Collection.open('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json')

for path in col.children():
    print('Path=%s' % path.id)
    for row in path.children():
        children = row.links('child')
        for c in children:
            print('aws s3 rm %s' % c.replace('https://landsat-stac.s3.amazonaws.com', 's3://landsat-stac'))
        # remove child links
        row.data['links'] = [l for l in row.data['links'] if l['rel'] != 'child']
        if len(children) > 0: 
            row.save()
            print('Saved %s' % row.filename)
