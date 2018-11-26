from simple_elastic import ElasticIndex
from typing import Dict, Tuple, List, Optional
import logging


def enrich(index: ElasticIndex, system_number: str) -> Tuple[Dict[str, int], Optional[str], Optional[List[str]]]:

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
        logging.debug(results)
        if 'doi' in results[0]:
            doi = results[0]['doi']
        else:
            doi = None
        return results[0]['hits'], doi, []
    else:
        return {'2012': 0, '2013': 0, '2014': 0, '2015': 0, '2016': 0, '2017': 0, '2018': 0, 'total': 0}, None, []
