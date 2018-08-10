from kafka_event_hub.producers import SRUProducer

if __name__ == '__main__':
    p = SRUProducer('config/dsv05_dump.yml')
    p.set_query_id_equal_with('HAN*')
    p.process()
