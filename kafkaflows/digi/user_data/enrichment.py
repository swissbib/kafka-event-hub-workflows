from simple_elastic import ElasticIndex
from kafkaflows.digi.user_data import swissbib, aleph, e_codices, e_manuscripta, e_rara, opac

host = 'http://localhost:9200'

e_codices_index = ElasticIndex('e-codices-data', 'hits', url=host)
e_manuscripta_index = ElasticIndex('e-manuscripta-data', 'hits', url=host)
e_rara_index = ElasticIndex('e-rara-data', 'hits', url=host)
aleph_index = ElasticIndex('aleph-dsv01-data', 'hits', url=host)
opac_index = ElasticIndex('opac-access', 'log', url=host)


def enrich_user_data(config):

    for index in config['indexes']:
        instance = ElasticIndex(**index['index'])

        query = {
            'query': {
                'match_all': {}
            }
        }

        for results in instance.scroll(query=query):
            for item in results:
                identifier = item['identifier']
                database = item['database']
                sys_number = item['identifiers'][database]

                if 'error_tags' in item:
                    item['error_tags'] = set(item['error_tags'])

                total = 0

                # swissbib
                hits, error_tags = swissbib.enrich(identifier)
                item['hits']['swissbib'] = hits
                total += hits['total']
                for tag in error_tags:
                    item['error_tags'].add(tag)

                # opac
                hits, error_tags = opac.enrich(opac_index, sys_number)
                item['hits']['opac-access'] = hits
                total += hits['total']
                for tag in error_tags:
                    item['error_tags'].add(tag)

                # aleph
                hits, error_tags = aleph.enrich(aleph_index, sys_number, database)
                item['hits']['aleph'] = hits
                total += hits['loans']['total']
                for tag in error_tags:
                    item['error_tags'].add(tag)

                if database == 'dsv05':
                    # e-rara
                    hits, error_tags = e_rara.enrich(e_rara_index, sys_number)
                    item['hits']['e-rara'] = hits
                    total += hits['bau']['total']
                    for tag in error_tags:
                        item['error_tags'].add(tag)

                    # e-manuscripta
                    hits, error_tags = e_manuscripta.enrich(e_manuscripta_index, sys_number)
                    item['hits']['e-manuscripta'] = hits
                    total += hits['bau']['total']
                    total += hits['swa']['total']
                    for tag in error_tags:
                        item['error_tags'].add(tag)

                    # e-codices
                    hits, doi, error_tags = e_codices.enrich(e_codices_index, sys_number)
                    item['hits']['e-codices'] = hits
                    total += hits['total']
                    for tag in error_tags:
                        item['error_tags'].add(tag)

                    if doi is not None:
                        if 'doi' in item['identifiers']:
                            if isinstance(item['identifiers']['doi'], list):
                                item['identifiers']['doi'].append(doi)
                            else:
                                item['identifiers']['doi'] = [item['identifiers']['doi'], doi]

                # e-mails dsv05
                # TODO

                item['error_tags'] = list(item['error_tags'])

                item['hits']['total'] = total

                instance.index_into(item, item['identifier'])
