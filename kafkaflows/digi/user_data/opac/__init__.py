from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List


def enrich(index: ElasticIndex, system_number: str) -> Tuple[Dict[str, int], Union[List[str], None]]:

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