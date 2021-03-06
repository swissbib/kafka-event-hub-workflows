from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List


swissbib_host = 'localhost:8080'


def enrich(system_number: str) -> Tuple[Dict[str, Dict[str, int]], Union[List[str], None]]:
    total = 0
    hits = dict()
    total_sub = 0
    for year in range(2018, 2019):
        sru = ElasticIndex('sru-{}'.format(year), doc_type='logs',
                           url=swissbib_host)
        hits['sru'] = dict()
        query = {'query': {'match': {'requestparams': system_number}}}
        num = len(sru.scan_index(query=query))
        hits['sru'][str(year)] = num
        total += num
        total_sub += num
    hits['sru']['total'] = total_sub

    for source in ['green', 'jus', 'bb']:
        hits[source] = dict()
        total_sub = 0
        for year in range(2017, 2019):
            swissbib = ElasticIndex('swissbib-{}-{}'.format(source, year),
                                    doc_type='logs',
                                    url=swissbib_host)

            query = {'query': {'term': {'request_middle.keyword': {'value': system_number}}}}
            num = len(swissbib.scan_index(query=query))
            hits[source][str(year)] = num
            total += num
            total_sub += num
        hits[source]['total'] = total_sub

    hits['total'] = total
    return hits, []