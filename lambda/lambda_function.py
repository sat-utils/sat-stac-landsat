import boto3
import json
import logging
import os
import requests
import sys

from datetime import datetime
from satstac import STACError
from satstac.landsat import transform

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

logger = logging.getLogger(__name__)


client = boto3.client('sns')

sns_arn = 'arn:aws:sns:us-west-2:552188055668:landsat-stac'


def lambda_handler(event, context):
    msg = json.loads(event['Records'][0]['Sns']['Message'])
    for m in msg['Records']:
        url = 'https://%s.s3.amazonaws.com/%s' % (m['s3']['bucket']['name'], m['s3']['object']['key'])
        print(m)
        id = os.path.splitext(os.path.basename(os.path.dirname(url)))[0]
        print(url, id)
        data = {
            'id': id,
            'datetime': datetime.strptime(id.split('_')[3], '%Y%m%d'),
            'url': url
        }
        stac = transform(data)
        print(data)
        print(stac)
        client.publish(Message=json.dumps(stac), TopicArn=sns_arn)
