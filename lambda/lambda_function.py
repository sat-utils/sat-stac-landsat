import logging
import sys

from satstac.landsat import transform

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    logger.info(event)
    logger.info(context)