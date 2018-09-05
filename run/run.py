import os
import sys
import argparse
import logging
import yaml
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafkaflows.digi.dsv01 import dsv01_producer, dsv01_producer_full, dsv01_consumer
from kafkaflows.digi.dsv05 import dsv05_consumer, dsv05_producer_full, dsv05_producer
from kafkaflows.digi.digispace import digispace_consumer, digispace_producer
from kafkaflows.digi.user_data import enrich_user_data

parser = argparse.ArgumentParser(description='CLI for Kafka Workflows')
parser.add_argument('script', action='store')

args = parser.parse_args()

config = yaml.load(open('configs/{}/base.yml'.format(args.script), mode='r'))

logging.basicConfig(filename='logs/{}.log'.format(args.script), filemode='w', level=config['logging']['level'],
                    format='%(levelname)s|%(name)s|%(asctime)s|%(message)s')

funcs = {
    'dsv05-consumer': dsv05_consumer,
    'dsv05-producer': dsv05_producer,
    'dsv05-producer-full': dsv05_producer_full,
    'dsv01-consumer': dsv01_consumer,
    'dsv01-producer': dsv01_producer,
    'dsv01-producer-full': dsv01_producer_full,
    'digidata-producer': digispace_producer,
    'digidata-consumer': digispace_consumer,
    'enrich-user-data': enrich_user_data

}

try:
    funcs[args.script](config)
except Exception as e:
    logging.exception(e)
    sys.exit(1)
else:
    logging.info('Application ended without error!')

