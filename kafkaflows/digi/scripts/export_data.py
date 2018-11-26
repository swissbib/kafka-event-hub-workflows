from simple_elastic import ElasticIndex
import json


if __name__ == '__main__':
    data = ElasticIndex('e-codices-data', 'hits')
    data.dump(".")