from simple_elastic import ElasticIndex

if __name__ == '__main__':
    data_dsv05 = ElasticIndex('data-*', 'record')
    opac = ElasticIndex('opac-access', 'log')

    for results in opac.scroll():
        for event in results:
            record = data_dsv05.get(event['system_number'])
            if record is not None:
                if 'opac_access' in record:
                    record['opac_access'] += 1
                else:
                    record['opac_access'] = 1
                data_dsv05.index_into(record, record['identifier'])


