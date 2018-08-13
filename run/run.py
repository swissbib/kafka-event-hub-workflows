import os
import sys
import argparse
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafkaflows.digi.dsv01 import run_dsv01_producer
from kafkaflows.digi.dsv01 import run_dsv01_consumer
from kafkaflows.digi.dsv05 import run_dsv05_consumer
from kafkaflows.digi.dsv05 import run_dsv05_producer

parser = argparse.ArgumentParser(description='CLI for Kafka Workflows')
parser.add_argument('script', action='store')
parser.add_argument('-d', action='store_true', dest='debug')

args = parser.parse_args()

if hasattr(args, 'debug'):
    logging.basicConfig(filename='logs/{}.log'.format(args.script), filemode='w', level=logging.DEBUG,
                        format='%(levelname)s|%(name)s|%(asctime)s|%(message)s')
else:
    logging.basicConfig(filename='logs/{}.log'.format(args.script), filemode='w', level=logging.ERROR,
                        format='%(levelname)s|%(name)s|%(asctime)s|%(message)s')

try:
    if args.script == 'dsv05-consumer':
        run_dsv05_consumer()
    elif args.script == 'dsv05-producer':
        run_dsv05_producer()
    elif args.script == 'dsv01-producer':
        run_dsv01_producer()
    elif args.script == 'dsv01-consumer':
        logging.debug('Start dsv01 consumer!')
        run_dsv01_consumer()
except Exception as e:
    logging.exception(e)
    sys.exit(1)
else:
    logging.info('Application ended without error!')

