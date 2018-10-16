from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List

placeholder = {
    'reservations': {'2016': 0, '2017': 0, '2018': 0, 'total': 0},
    'loans': {'2016': 0, '2017': 0, '2018': 0, 'total': 0}
}


def enrich(index: ElasticIndex, system_number: str, database: str) -> Tuple[Dict[str, Dict[str, int]], Union[List[str], None]]:
    if database == 'dsv01':
        query = {
            '_source': ['reservations.*', 'loans.*'],
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
            return placeholder, ['_no_aleph_data']
    else:
        # place holder values for scripted fields.
        return placeholder, ['_no_aleph_data']