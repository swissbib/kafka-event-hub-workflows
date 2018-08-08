from project_digitale_nutzung.utility.mapper import MARCMapper
from project_digitale_nutzung.utility.vufind_format_codes import swissbib_format_codes

from pymarc.reader import JSONReader
from simple_elastic import ElasticIndex

import re
import os
import sys
import logging


if __name__ == '__main__':
    dsv01_dig = ElasticIndex('dsv01-digitised', 'marc')
    index = ElasticIndex('data-dsv01', 'record')
    format_dict = swissbib_format_codes()

    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)
    logger = logging.getLogger(__name__)

    for root, dirs, files in os.walk('data/dsv01/'):
        for file in files:

            with open(root + file, 'r') as fp:
                reader = JSONReader(fp)
                for record in reader:
                    marc = MARCMapper(record)

                    for field in marc.get_fields('949'):
                        if field['F'] in ['A100', 'A125', 'A130']:
                            marc.add_value('library', field['F'])
                            marc.add_identifier('call_number', field['j'])

                    if 'library' not in marc.result:
                        continue

                    marc.add_value('database', 'dsv01')

                    marc.identifier()

                    marc.parse_leader()
                    marc.parse_cat_date()

                    date_tag = marc.parse_008_date()
                    if date_tag == 'after_1920':
                        continue

                    marc.parse_rest_008()

                    for _035 in record.get_fields('035'):
                        if _035['a'] is not None:
                            if _035['a'].startswith('(IDSBB)'):
                                marc.add_identifier('dsv01', _035['a'].split(')')[1])

                    marc.parse_field('245', 'a', 'title')
                    marc.parse_field('245', 'b', 'subtitle')
                    marc.parse_field('245', 'c', 'author')
                    marc.parse_field('245', 'h', 'print_material')

                    marc.parse_field_to_subfield('264', 'a', 'production', 'place')
                    marc.parse_field_to_subfield('264', 'b', 'production', 'publisher')
                    marc.parse_field_to_subfield('264', 'c', 'production', 'date')

                    marc.parse_field_to_subfield('300', 'a', 'extent', 'coverage')
                    marc.parse_field_to_subfield('300', 'b', 'extent', 'physical_attributes')
                    marc.parse_field_to_subfield('300', 'c', 'extent', 'size_and_format')
                    marc.parse_field_to_subfield('300', 'e', 'extent', 'additional_content')

                    marc.parse_field_append_to_subfield('336', 'a', 'extent', 'content')
                    marc.parse_field_append_to_subfield('337', 'a', 'extent', 'media')
                    marc.parse_field_append_to_subfield('338', 'a', 'extent', 'carrier')
                    marc.parse_field_to_subfield('348', 'a', 'extent', 'music')

                    marc.parse_field_list(['600', '610', '611', '630', '648', '650', '651', '653', '655', '690', '691'],
                                          {'a': 'title', '2': 'source', '0': 'identifier'}, 'subject_headings')

                    marc.parse_field('856', 'u', 'link')

                    marc.parse_field('898', 'a', 'a-format')
                    if 'a-format' in marc.result:
                        marc.result['a-format'] = format_dict[marc.result['a-format']]
                    marc.parse_field('898', 'b', 'b-format')
                    if 'b-format' in marc.result:
                        marc.result['b-format'] = format_dict[marc.result['b-format']]
                    marc.parse_field('898', 'c', 'c-format')
                    if 'c-format' in marc.result:
                        marc.result['c-format'] = format_dict[marc.result['c-format']]

                    query = {"_source": False, "query": {"term": {"_id": marc.result['identifier']}}}
                    if len(dsv01_dig.search(query=query)) > 0:
                        marc.add_value('is_digitized', True)
                    else:
                        marc.add_value('is_digitized', False)

                    marc.add_value_sub('final', 'type', 'print')

                    if 'coverage' in marc.result['extent']:
                        matches = re.findall('\d+', marc.result['extent']['coverage'])
                        _sum = 0
                        for match in matches:
                            _sum += int(match)
                        if re.search('Bd.|B[äa]nd[e]?|[Tt]omes|T((h)?eil)?.|[Vv]ol.|[Hh]eft(en)?', marc.result['extent']['coverage']):
                            marc.add_value_sub_sub('final', 'extent', 'unit', _sum)
                            marc.add_value_sub_sub('final', 'extent', 'unit_name', 'Band')
                        else:
                            marc.add_value_sub_sub('final', 'extent', 'unit', 1)
                            marc.add_value_sub_sub('final', 'extent', 'unit_name', 'Dossier')
                            marc.add_value_sub_sub('final', 'extent', 'sub_unit', _sum)
                            marc.add_value_sub_sub('final', 'extent', 'sub_unit_name', 'Seiten/Blätter')
                    else:
                        marc.add_error_tag('_no_coverage')
                        marc.add_value_sub_sub('final', 'extent', 'unit', 1)
                        marc.add_value_sub_sub('final', 'extent', 'unit_name', 'Dossier')

                    if date_tag == 'ok':
                        year = marc.result['dates']['date']['year']
                        marc.add_value_sub('final', 'year', int(year))
                        marc.append_value_sub('final', 'century', int(year / 100) + 1)
                    else:
                        marc.add_error_tag('_no_valid_date')

                    index.index_into(marc.result, marc.result['identifier'])






