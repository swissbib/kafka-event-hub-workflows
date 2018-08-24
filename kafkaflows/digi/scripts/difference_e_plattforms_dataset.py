from simple_elastic import ElasticIndex
from collections import Counter
import json

if __name__ == '__main__':
    c = Counter()

    index = ElasticIndex('kafka*', 'record')
    with open('data/collected-hits-e-plattforms.json', 'r') as fp:
        data = json.load(fp)
        for sys_number in data:
            query = {
                'query': {
                    'bool':{
                        'should': [
                            {
                                'match': {
                                    'identifiers.dsv05': sys_number
                                }
                            },
                            {
                                'match': {
                                    'identifiers.dsv01': sys_number
                                }
                            }
                        ],
                        'minimum_should_match': 1
                    }
                }
            }
            results = index.scan_index(query=query)

            if len(results) == 1:
                c['found'] += 1
            elif len(results) == 0:
                c['not_found'] += 1
                print('NOT FOUND: ' + sys_number)
            elif len(results) == 2:
                c['two-found'] += 1
                print('TWO MATCHES: ' + sys_number)

    for x in c.items():
        print(x)
