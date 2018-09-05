from kafka_event_hub.consumers import ElasticConsumer
from kafka_event_hub.consumers.utility import DataTransformation

import logging
import json

db_translation = {
    '1': 'dsv01',
    '5': 'dsv05'
}


class Transformation(DataTransformation):

    def __init__(self):
        super().__init__()

    def transform(self, value: str):
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
            while len(sys_number) < 9:
                sys_number = '0' + sys_number
            result['system_number'] = sys_number
            if 'images' in record:
                result['number_of_images'] = record['images']

        return result


def digispace_consumer(config):
    logging.debug('Create digidata elastic index.')
    consumer = ElasticConsumer(config['consumer.path'], transformation_class=Transformation)
    while True:
        consumer.consume(num_messages=100)



