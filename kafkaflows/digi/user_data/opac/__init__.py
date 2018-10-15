from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List

opac_host = 'http://localhost:9200'


def enrich(system_number: str) -> Tuple[Dict[str, int], Union[List[str], None]]:
    index = ElasticIndex('opac-access', 'log', url=opac_host)

    query = {
            'query': {
                'term': {
                    'system_number': {
                        'value': system_number
                    }
                }
            }
        }
    hits = len(index.scan_index(query=query))
    identifier = int(system_number)
    if identifier < 320000:
        return {'total': hits}, ['_opac_dual_hit']
    else:
        return {'total': hits}, []