from simple_elastic import ElasticIndex
from kafkaflows.digi.user_data import swissbib, aleph, e_plattforms, opac


if __name__ == '__main__':
    index = ElasticIndex('kafka*', 'record', url='http://sb-ues2.swissbib.unibas.ch:9200')

    for results in index.scroll():
        for item in results:

            document = dict()
            identifier = item['identifier']
            sys_number = item['identifiers'][item['database']]

            document['hits'] = dict()
            error_tags_list = list()
            hits, error_tags = swissbib.enrich(sys_number)
            document['hits']['swissbib'] = hits
            error_tags_list.extend(error_tags)

            hits, error_tags = opac.enrich(sys_number)
            document['hits']['opac-access'] = hits
            error_tags_list.extend(error_tags)

            index.update(document, doc_id=identifier)
            for tag in error_tags_list:
                index.script_update('ctx._source.error_tags.add(params.tag)', {'tag': tag}, doc_id=identifier)
