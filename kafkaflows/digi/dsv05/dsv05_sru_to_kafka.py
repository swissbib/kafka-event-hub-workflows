from kafka_event_hub.producers import SRUProducer


def run_dsv05_producer(config):
    p = SRUProducer(config['producer.path'])
    p.set_query_id_equal_with('HAN*')
    p.process()
