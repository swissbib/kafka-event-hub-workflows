from simple_elastic import ElasticIndex

import json

if __name__ == '__main__':

    dsv01 = set()
    dsv05 = set()

    index_aleph = ElasticIndex('alepth-dsv01-data', 'hits', url='http://sb-ues2.swissbib.unibas.ch:9200')
    index_core = ElasticIndex('kafka-*', 'record', url='http://sb-ues2.swissbib.unibas.ch:9200')
    index_e_plattforms = ElasticIndex('e-data', 'hits', url='http://sb-ues2.swissbib.unibas.ch:9200')

    index_digidata = ElasticIndex('add-digidata', 'data', url='http://sb-ues2.swissbib.unibas.ch:9200')

    query = {
        '_source': ['system_number'],
        'query': {
            'term': {
                'database': 'dsv01'
            }
        }
    }

    results = index_digidata.scan_index(query=query)
    for item in results:
        dsv01.add(item['system_number'])

    query['query']['term']['database'] = 'dsv05'

    results = index_digidata.scan_index(query=query)
    for item in results:
        dsv05.add(item['system_number'])

    results = index_aleph.scan_index()
    for item in results:
        dsv01.add(item['identifier'])

    query = {
        '_source': ['identifiers.dsv01', 'identifiers.dsv05'],
        'query': {
            'match_all': {}
        }
    }
    results = index_core.scan_index()
    for item in results:
        if 'dsv01' in item['identifiers']:
            dsv01.add(item['identifiers']['dsv01'])
        if 'dsv05' in item['identifiers']:
            dsv05.add(item['identifiers']['dsv05'])

    results = index_e_plattforms.scan_index()
    for item in results:
        if ('emanus-swa' or 'emanus-bau') in results:
            dsv05.add(results['identifier'])
        elif 'erara-bau' in results:
            dsv01.add(results['identifier'])

    with open('data/dsv01_system_numbers.json', 'w') as fp:
        json.dump(list(dsv01), fp, indent='    ', ensure_ascii=False)

    with open('data/dsv05_system_numbers.json', 'w') as fp:
        json.dump(list(dsv05), fp, indent='    ', ensure_ascii=False)