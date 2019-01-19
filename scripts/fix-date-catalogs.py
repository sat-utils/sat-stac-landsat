#!/usr/bin/env python

'''
A recent changed caused date sub-catalogs to be created, but there should not be any.
The row catalog should contain `item` links directly to the items, and not have any child
links to date catalogs.

This script takes the items under the date sub-catalogs and adds them to the proper
parent catalog. It does not remove the date catalog links, that's to be another script
'''

from satstac import Catalog, Collection

col = Collection.open('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json')

for path in col.children():
    print('Path=%s' % path.id)
    for row in path.children():
        for child in row.children():
            for item in child.items():
                col.add_item(item, path='${eo:column}/${eo:row}', filename= '${date}/${id}')
        #print('rm %s' % child.filename)
        # remove child links
        #links = [l for l in row.data['links'] if l['rel'] != 'child']
