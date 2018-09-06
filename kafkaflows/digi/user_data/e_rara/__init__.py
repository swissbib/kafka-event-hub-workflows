from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List

e_data_host = 'http://sb-ues2.swissbib.unibas.ch:9200'


def enrich(system_number: str) -> Tuple[Dict[str, Dict[str, int]], Union[List[str], None]]:
    index = ElasticIndex('e-rara-data', 'log', url=e_data_host)

    query = {
            '_source': ['bau.*'],
            'query': {
                'term': {
                    'system_number': {
                        'value': system_number
                    }
                }
            }
        }
    hits = index.scan_index(query=query)[0]
    return hits, []