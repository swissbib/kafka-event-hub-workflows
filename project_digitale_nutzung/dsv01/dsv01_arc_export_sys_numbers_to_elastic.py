from simple_elastic import ElasticIndex

from datetime import date

dsv01_full_export = ElasticIndex('dsv01-sys-numbers-before-1900', 'record')

with open('data/dsv01_system_numbers_vor_1900_arc_export_20180802.csv', 'r', encoding='utf-16') as file:
    for line in file:
        year, sys_number = line.split(',')
        doc = dict()
        while len(sys_number) != 10:
            sys_number = '0' + sys_number
        doc['system_number'] = sys_number.strip()
        doc['publication_date'] = year
        doc['index_date'] = date.today().isoformat()
        dsv01_full_export.index_into(doc, doc['system_number'])