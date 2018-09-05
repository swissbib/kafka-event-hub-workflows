from kafkaflows.digi.utility.transformation import TransformSruExport
from kafka_event_hub.consumers import ElasticConsumer

import logging


def dsv01_consumer(config):
    logger = logging.getLogger(__name__)
    consumer = ElasticConsumer(config['consumer.path'], transformation_class=TransformSruExport, logger=logger)
    while True:
        consumer.consume()



