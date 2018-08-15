from kafka_event_hub.consumers import ElasticConsumer

import logging
import json

db_translation = {
    '1': 'dsv01',
    '5': 'dsv05'
}


def digispace_data_transformation(value: str) -> dict:
    record = json.loads(value, encoding='utf-8')

    result = dict()
    sys_id = record['sys_id']
    if isinstance(sys_id, list):
        sys_id = sys_id[0]

    result['identifier'] = sys_id
    try:
        db, sys_number = sys_id.split('_')
    except ValueError:
        pass
    else:
        result['database'] = db_translation[db]
        result['system_number'] = sys_number
        if 'images' in record:
            result['number_of_images'] = record['images']

    return result


def run_digispace_kafka_to_result(config):
    logging.debug('Create digidata elastic index.')
    consumer = ElasticConsumer(config['consumer.path'])
    consumer.set_transformation_policy(digispace_data_transformation)

    while True:
        consumer.consume(num_messages=100)



