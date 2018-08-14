import os
import sys
import argparse
import logging
import yaml
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafkaflows.digi.dsv01 import run_dsv01_producer
from kafkaflows.digi.dsv01 import run_dsv01_consumer
from kafkaflows.digi.dsv05 import run_dsv05_consumer
from kafkaflows.digi.dsv05 import run_dsv05_producer
from kafkaflows.digi.swissbib_elk import run_swissbib_elk

parser = argparse.ArgumentParser(description='CLI for Kafka Workflows')
parser.add_argument('script', action='store')

args = parser.parse_args()

config = yaml.load(open('configs/{}/base.yml'.format(args.script), mode='r'))


logging.basicConfig(filename='logs/{}.log'.format(args.script), filemode='w', level=config['logging']['level'],
                    format='%(levelname)s|%(name)s|%(asctime)s|%(message)s')

try:
    if args.script == 'dsv05-consumer':
        run_dsv05_consumer(config)
    elif args.script == 'dsv05-producer':
        run_dsv05_producer(config)
    elif args.script == 'dsv01-producer':
        run_dsv01_producer(config)
    elif args.script == 'dsv01-consumer':
        run_dsv01_consumer(config)
    elif args.script == 'swissbib-elk':
        run_swissbib_elk(config)
except Exception as e:
    logging.exception(e)
    sys.exit(1)
else:
    logging.info('Application ended without error!')

