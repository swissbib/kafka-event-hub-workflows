from kafka_event_hub.producers import SRUProducer


def run_dsv05_producer():
    p = SRUProducer('configs/dsv05/dsv05_dump.yml')
    p.set_query_id_equal_with('HAN*')
    p.process()