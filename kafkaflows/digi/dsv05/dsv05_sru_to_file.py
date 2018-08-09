from kafka_event_hub.producers import SRUProducer

if __name__ == '__main__':
    p = SRUProducer('config/dsv05_dump.yml')
    p.query_id_equal('HAN*')
    p.process()
