from simple_elastic import ElasticIndex
from typing import Dict, Tuple, Union, List
import logging

e_data_host = 'http://localhost:9200'


def enrich(system_number: str) -> Tuple[Dict[str, int], Union[str, None], Union[List[str], None]]:
    index = ElasticIndex('e-codices-data', 'hits', url=e_data_host)

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


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    print('start')
    hits, doi, error_tags = enrich('000083898')
    print(hits, doi, error_tags)