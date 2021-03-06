from kafka_event_hub.producers import SRUProducer
import json


def dsv05_producer_full(config):
    p = SRUProducer(config['producer.path'])
    p.set_query_id_equal_with('HAN*')
    p.process()


def dsv05_producer(config):
    with open('data/dsv05_system_numbers.json', 'r') as file:
        producer = SRUProducer(config['producer.path'])
        sys_numbers = json.load(file)
        for sys_number in sys_numbers:
            while len(sys_number) < 9:
                sys_number = '0' + sys_number
            producer.set_query_id_equal_with('HAN' + sys_number)
            producer.process()
