from simple_elastic import ElasticIndex

import logging


def run_swissbib_elk(config):
    logger = logging.getLogger(__name__.split('.')[-1])

    kafka_dsv01 = ElasticIndex('kafka-dsv01', doc_type='record', url=config['elastic']['target.host'])
    logger.info('Connected to kafka-dsv01.')

    kafka_dsv05 = ElasticIndex('kafka-dsv05', doc_type='record', url=config['elastic']['target.host'])
    logger.info('Connected to kafka-dsv05')

    for instance in kafka_dsv01, kafka_dsv05:
        logger.debug('Get clicks for %s.', instance.index)
        for results in instance.scroll():
            for record in results:
                identifier = record['identifier']

                hits = dict()
                for year in range(2017, 2019):
                    sru = ElasticIndex('sru-{}'.format(year), doc_type='logs',
                                       url=config['elastic']['swissbib.host'])
                    hits['sru'] = dict()
                    query = {'query': {'match': {'requestparams': identifier}}}
                    hits['sru'][str(year)] = len(sru.scan_index(query=query))

                for source in ['green', 'jus', 'bb']:
                    hits[source] = dict()
                    for year in range(2017, 2019):
                        swissbib = ElasticIndex('swissbib-{}-{}'.format(source, year),
                                                doc_type='logs',
                                                url=config['elastic']['swissbib.host'])

                        query = {'query': {'term': {'request_middle.keyword': {'value': identifier}}}}
                        hits[source][str(year)] = len(swissbib.scan_index(query=query))

                record['hits'] = hits
                instance.index_into(record, identifier)



