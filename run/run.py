import os
import sys
import argparse
import logging
import yaml
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafkaflows.digi.dsv01 import run_dsv01_producer, run_general_dsv01_a100_producer, run_general_dsv01_a125_producer, run_dsv01_consumer
from kafkaflows.digi.dsv05 import run_dsv05_consumer, run_dsv05_producer, run_dsv05_producer_pre_compiled_list
from kafkaflows.digi.digispace import run_digispace_kafka_to_result, run_digispace_to_kafka
from kafkaflows.digi.user_data import enrich

parser = argparse.ArgumentParser(description='CLI for Kafka Workflows')
parser.add_argument('script', action='store')

args = parser.parse_args()

config = yaml.load(open('configs/{}/base.yml'.format(args.script), mode='r'))


logging.basicConfig(filename='logs/{}.log'.format(args.script), filemode='w', level=config['logging']['level'],
                    format='%(levelname)s|%(name)s|%(asctime)s|%(message)s')

try:
    if args.script == 'dsv05-consumer':
        run_dsv05_consumer(config)
    elif args.script == 'dsv05-producer-full':
        run_dsv05_producer(config)
    elif args.script == 'dsv05-producer':
        run_dsv05_producer_pre_compiled_list(config)
    elif args.script == 'dsv01-producer':
        run_dsv01_producer(config)
    elif args.script == 'dsv01-producer-full':
        run_general_dsv01_a100_producer(config)
        run_general_dsv01_a125_producer(config)
    elif args.script == 'dsv01-consumer':
        run_dsv01_consumer(config)
    elif args.script == 'digispace-producer':
        run_digispace_to_kafka(config)
    elif args.script == 'digispace-consumer':
        run_digispace_kafka_to_result(config)
    elif args.script == 'enrich':
        enrich()
except Exception as e:
    logging.exception(e)
    sys.exit(1)
else:
    logging.info('Application ended without error!')

