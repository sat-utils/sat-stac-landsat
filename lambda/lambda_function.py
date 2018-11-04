import logging

from satstac.landsat import transform

logger = logging.getLogger(__name__)


def lambda_handler(event):
    logger.info(event)
    
    