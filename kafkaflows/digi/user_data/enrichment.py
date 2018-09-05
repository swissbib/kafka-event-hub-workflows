from simple_elastic import ElasticIndex
from kafkaflows.digi.user_data import swissbib, aleph, e_plattforms, opac


def enrich_user_data(config):

    for index in config['indexes']:
        instance = ElasticIndex(**index['index'])

        for results in instance.scroll():
            for item in results:
                document = dict()
                identifier = item['identifier']
                database = item['database']
                sys_number = item['identifiers'][database]

                total = 0
                document['hits'] = dict()
                error_tags_list = list()

                instance.script_update("ctx._source.remove('hits')", dict(), doc_id=identifier)

                # swissbib
                hits, error_tags = swissbib.enrich(identifier)
                document['hits']['swissbib'] = hits
                total += hits['total']
                error_tags_list.extend(error_tags)

                # opac
                hits, error_tags = opac.enrich(sys_number)
                document['hits']['opac-access'] = hits
                total += hits['total']
                error_tags_list.extend(error_tags)

                # aleph
                # hits, error_tags = aleph.enrich(sys_number, database)
                # document['hits']['aleph'] = hits
                # total += hits['total']
                # error_tags_list.extend(error_tags)

                # e-plattforms
                # hits, error_tags = e_rara.enrich(sys_number, database)
                # document['hits']['e-rara'] = hits
                # total += hits['total']
                # error_tags_list.extend(error_tags)

                document['hits']['total'] = total

                instance.update(document, doc_id=identifier)
                for tag in error_tags_list:
                    instance.script_update('ctx._source.error_tags.add(params.tag)', {'tag': tag}, doc_id=identifier)
