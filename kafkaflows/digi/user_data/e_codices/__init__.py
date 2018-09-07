from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List

e_data_host = 'http://sb-ues2.swissbib.unibas.ch:9200'


def enrich(system_number: str) -> Tuple[Dict[str, int], Union[str, None], Union[List[str], None]]:
    index = ElasticIndex('e-codices-data', 'log', url=e_data_host)

    query = {
            '_source': ['hits.*', 'doi'],
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
        if 'doi' in results[0]:
            doi = results[0]['doi']
        else:
            doi = None
        return results[0]['hits'], doi, []
    else:
        return {'2012': 0, '2013': 0, '2014': 0, '2015': 0, '2016': 0, '2017': 0, '2018': 0, 'total': 0}, None, []
