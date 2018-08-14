from kafka_event_hub.producers import ElasticProducer


def run_digispace_to_kafka(config):
    producer = ElasticProducer(config['producer.path'])
    producer.process()
