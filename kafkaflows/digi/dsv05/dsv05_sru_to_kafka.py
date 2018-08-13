from kafka_event_hub.producers import SRUProducer

import logging



if __name__ == '__main__':
    logging.basicConfig(filename='logs/dsv05-producer.log', filemode='w', level=logging.INFO)
    p = SRUProducer('config/dsv05_dump.yml')
    p.set_query_id_equal_with('HAN*')
    p.process()
