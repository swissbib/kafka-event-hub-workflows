from simple_elastic import ElasticIndex
import json


if __name__ == '__main__':
    data = ElasticIndex('e-codices-data', 'hits')

    export = list()
    for results in data.scroll():
        export.extend(results)
        print(len(export))
    print(len(export))

    with open('elias-data-e-codices-20181022.json', 'w') as fp:
        json.dump(export, fp)