from kafka_event_hub.producers import SRUProducer
import logging


def run_dsv01_producer():
    with open('data/dsv01_system_numbers_vor_1900_arc_export_20180802.csv', 'r', encoding='utf-16') as file:
        producer = SRUProducer('configs/dsv01/dsv01_dump.yml')
        for line in file:
            year, sys_number = line.split(',')
            while len(sys_number) != 10:
                sys_number = '0' + sys_number
            if sys_number != '000013825':
                producer.set_query_anywhere_equal_with('IDSBB' + sys_number)
                producer.process()


