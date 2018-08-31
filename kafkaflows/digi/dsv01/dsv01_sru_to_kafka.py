from kafka_event_hub.producers import SRUProducer
import logging


def run_dsv01_producer(config):
    with open('data/dsv01_system_numbers.json', 'r') as file:
        producer = SRUProducer(config['producer.path'])
        for line in file:
            year, sys_number = line.split(',')
            while len(sys_number) != 10:
                sys_number = '0' + sys_number
            if sys_number != '000013825':
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
