from simple_elastic import ElasticIndex
from kafkaflows.digi.user_data import swissbib, aleph, e_plattforms, opac


def enrich_user_data(config):

    for index in config['indexes']:
        instance = ElasticIndex(**index['index'])

        for results in instance.scroll():
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

                instance.update(document, doc_id=identifier)
                for tag in error_tags_list:
                    instance.script_update('ctx._source.error_tags.add(params.tag)', {'tag': tag}, doc_id=identifier)
