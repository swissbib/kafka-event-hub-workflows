from kafka_event_hub.producers import SRUProducer
import logging
import json


def run_dsv01_producer(config):
    with open('data/dsv01_system_numbers.json', 'r') as file:
        producer = SRUProducer(config['producer.path'])
        sys_numbers = json.load(file)
        for sys_number in sys_numbers:
            while len(sys_number) < 10:
                sys_number = '0' + sys_number
            producer.set_query_anywhere_equal_with('IDSBB' + sys_number)
            producer.process()


def run_general_dsv01_a100_producer(config):
    producer = SRUProducer(config['producer.path'])
    producer.add_simple_and_query('dc.anywhere', '=', 'IDSBB*')
    producer.add_simple_and_query('dc.possessingInstitution', '=', 'A100')
    producer.add_simple_and_query('dc.date', '<=', '1920')
    producer.process()


def run_general_dsv01_a125_producer(config):
    producer = SRUProducer(config['producer.path'])
    producer.add_simple_and_query('dc.anywhere', '=', 'IDSBB*')
    producer.add_simple_and_query('dc.possessingInstitution', '=', 'A125')
    producer.add_simple_and_query('dc.date', '<=', '1920')
    producer.process()
