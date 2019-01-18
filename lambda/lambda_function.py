import boto3
import json
import logging
import os
import requests
import sys

from datetime import datetime
from satstac import STACError, Collection
from satstac.landsat import transform

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

client = boto3.client('sns', region_name='us-west-2')
kinesis_client = boto3.client('kinesis', region_name='us-west-2')

sns_arn = 'arn:aws:sns:us-west-2:552188055668:landsat-stac'


def lambda_handler(event, context):
    collection = Collection.open('https://landsat-stac.s3.amazonaws.com/landsat-8-l1/catalog.json')
    
    msg = json.loads(event['Records'][0]['Sns']['Message'])
    for m in msg['Records']:
        logger.info('Message: %s' % json.dumps(m))
        url = 'https://%s.s3.amazonaws.com/%s' % (m['s3']['bucket']['name'], m['s3']['object']['key'])
        id = os.path.splitext(os.path.basename(os.path.dirname(url)))[0]
        data = {
            'id': id,
            'datetime': datetime.strptime(id.split('_')[3], '%Y%m%d'),
            'url': url
        }
        # transform to STAC
        item = transform(data)
        logger.debug('Item: %s' % json.dumps(item.data))
        collection.add_item(item, path='${eo:column}/${eo:row}', filename= '${date}/${id}')
        logger.info('Added %s as %s' % (item, item.filename))
        data = json.dumps(item.data)
        client.publish(TopicArn=sns_arn, Message=data)
        logger.info('Published to %s' % sns_arn)
        # push to stream if envvar set
        stream = os.getenv('STREAM_NAME', None)
        if stream:
            resp = kinesis_client.put_record(StreamName=stream, Data=data, PartitionKey='one')
            logger.debug('Kinesis response: %s' % resp)
            
