from simple_elastic import ElasticIndex


def run_swissbib_elk():
    kafka_dsv01 = ElasticIndex('kafka-dsv01', doc_type='record', url='http://sb-ues2.swissbib.unibas.ch:9200')
    kafka_dsv05 = ElasticIndex('kafka-dsv05', doc_type='record', url='http://sb-ues2.swissbib.unibas.ch:9200')

    for instance in kafka_dsv01, kafka_dsv05:
        for record in instance.scroll():
            identifier = record['identifier']

            hits = dict()
            for year in range(2017, 2019):
                sru = ElasticIndex('sru-{}'.format(year), doc_type='logs', url='http://sb-uesl1.swissbib.unibas.ch:9200')
                hits['sru'] = dict()
                query = {'query': {'match': {'requestparams': identifier}}}
                hits['sru'][str(year)] = len(sru.scan_index(query=query))

            for source in ['green', 'jus', 'bb']:
                hits[source] = dict()
                for year in range(2017, 2019):
                    swissbib = ElasticIndex('swissbib-{}-{}'.format(source, year),
                                            doc_type='logs',
                                            url='http://sb-uesl1.swissbib.unibas.ch:9200')

                    query = {'query': {'term': {'request_middle.keyword': {'value': identifier}}}}
                    hits[source][str(year)] = len(swissbib.scan_index(query=query))

            record['hits'] = hits
            instance.index_into(record, identifier)



