from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List

e_data_host = 'http://sb-ues2.swissbib.unibas.ch:9200'


def enrich(system_number: str) -> Tuple[Dict[str, Dict[str, int]], Union[List[str], None]]:
    index = ElasticIndex('e-manuscripta-data', 'log', url=e_data_host)

    query = {
            '_source': ['bau.*', 'swa.*'],
            'query': {
                'term': {
                    '_id': {
                        'value': system_number
                    }
                }
            }
        }

    results = index.scan_index(query=query)
    if len(results) == 1:
        return results[0], []
    else:
        return {'bau': {'2016': 0, '2017': 0, '2018': 0, 'total': 0},
                'swa': {'2016': 0, '2017': 0, '2018': 0, 'total': 0}}, []
