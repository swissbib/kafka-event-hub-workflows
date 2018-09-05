from kafka_event_hub.producers import ElasticProducer


def digispace_producer(config):
    producer = ElasticProducer(config['producer.path'])
    producer.process()
