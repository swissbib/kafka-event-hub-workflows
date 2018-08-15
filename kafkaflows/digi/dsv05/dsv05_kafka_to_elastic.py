from kafka_event_hub.consumers import ElasticConsumer
from kafkaflows.digi.utility.transformation import TransformSruExport

import logging


def run_dsv05_consumer(config):
    logger = logging.getLogger(__name__)
    consumer = ElasticConsumer(config['consumer.path'], TransformSruExport, logger)
    while True:
        consumer.consume()
