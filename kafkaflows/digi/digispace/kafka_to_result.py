from kafka_event_hub.consumers import SimpleConsumer
from simple_elastic import ElasticIndex

import logging
import json

db_translation = {
    '1': 'dsv01',
    '5': 'dsv05'
}


def run_digispace_kafka_to_result(config):
    logging.debug('Begin adding digispace data to database.')
    index = ElasticIndex(**config['Elastic'])

    consumer = SimpleConsumer(config['consumer.path'])
    while True:
        for key, value in consumer.consume(num_messages=100):
            db, sys_number = key.split('_')

            query = {
                'query': {
                    'term': {
                        'identifiers.{}'.format(db_translation[db]): {
                            'value': sys_number
                        }
                    }
                }
            }
            results = index.scan_index(query=query)
            if len(results) == 1:
                digidata = json.loads(value)
                record = results[0]
                record['digidata'] = dict()
                record['digidata']['is_digitised'] = True
                if 'images' in digidata:
                    record['digidata']['images'] = digidata['images']
                index.index_into(record, record['identifier'])
            elif len(results) == 0:
                logging.error('Found no records for %s form %s.', sys_number, db_translation[db])
            else:
                logging.error('Found multiple results for %s from %s.', sys_number, db_translation[db])



