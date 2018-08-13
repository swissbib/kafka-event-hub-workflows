import os
import sys
import argparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafkaflows.digi.dsv01 import run_dsv01_producer
from kafkaflows.digi.dsv01 import run_dsv01_consumer
from kafkaflows.digi.dsv05 import run_dsv05_consumer
from kafkaflows.digi.dsv05 import run_dsv05_producer

parser = argparse.ArgumentParser(description='CLI for Kafka Workflows')
parser.add_argument('script', action='store')

args = parser.parse_args()

if args.script == 'dsv05-consumer':
    run_dsv05_consumer()
elif args.script == 'dsv05-producer':
    run_dsv05_producer()
elif args.script == 'dsv01-producer':
    run_dsv01_producer()
elif args.script == 'dsv01-consumer':
    run_dsv01_consumer()
