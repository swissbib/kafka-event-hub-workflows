from project_digitale_nutzung.utility.mapper import MARCMapper
from project_digitale_nutzung.utility.vufind_format_codes import swissbib_format_codes

from simple_elastic import ElasticIndex
from pymarc.reader import JSONReader

import logging
import sys
import os
import re

if __name__ == '__main__':
    dsv05_dig = ElasticIndex('dsv05-digitised', 'marc')
    index = ElasticIndex('data-dsv05', 'record')
    format_dict = swissbib_format_codes()

    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)
    logger = logging.getLogger(__name__)
    for root, dirs, files in os.walk('data/dsv05/'):
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

                    marc.add_value('database', 'dsv05')

                    marc.identifier()
                    marc.add_identifier('dsv05', marc['001'].value()[3:])

                    marc.parse_leader()
                    marc.parse_cat_date()

                    date_tag = marc.parse_008_date()
                    if date_tag == 'after_1920':
                        continue

                    marc.parse_rest_008()

                    if record['024'] is not None:
                        if record['024']['a'] is not None:
                            marc.add_identifier('doi', record['024']['a'])

                    date_tag_046 = 'no_date'
                    if record['046'] is not None:
                        marc.result['dates']['exact'] = dict()
                        if record['046']['a'] is not None:
                            marc.result['dates']['exact']['code'] = record['046']['a']
                        if record['046']['c'] is not None:
                            date = record['046']['c']
                            if re.match('\d{4}$', date):
                                marc.result['dates']['exact']['year'] = int(date)
                                if int(date) <= 1920:
                                    date_tag_046 = 'ok'
                                else:
                                    date_tag_046 = 'after_1920'
                            elif re.match('\d{4}\.\d{2}\.\d{2}$', date):
                                year, month, day = date.split('.')
                                marc.result['dates']['exact']['year'] = int(year)
                                marc.result['dates']['exact']['month'] = int(month)
                                marc.result['dates']['exact']['day'] = int(day)
                                if int(year) <= 1920:
                                    date_tag_046 = 'ok'
                                else:
                                    date_tag_046 = 'after_1920'
                            else:
                                date_tag_046 = 'unclear'
                                marc.add_error_tag('_no_valid_046_date')
                        if record['046']['e'] is not None:
                            marc.result['dates']['exact']['to'] = record['046']['e']
                        if record['046']['b'] is not None:
                            marc.result['dates']['exact']['from_b_Chr'] = record['046']['b']

                    marc.parse_field('245', 'a', 'title')
                    marc.parse_field('245', 'b', 'subtitle')
                    marc.parse_field('245', 'c', 'author')
                    marc.parse_field('245', 'h', 'print_material')

                    marc.parse_field_to_subfield('264', 'a', 'production', 'place')
                    marc.parse_field_to_subfield('264', 'b', 'production', 'publisher')
                    marc.parse_field_to_subfield('264', 'c', 'production', 'date')

                    marc.parse_field('250', 'a', 'version')

                    marc.parse_field_to_subfield('300', 'a', 'extent', 'coverage')
                    marc.parse_field_to_subfield('300', 'b', 'extent', 'physical_attributes')
                    marc.parse_field_to_subfield('300', 'c', 'extent', 'size_and_format')
                    marc.parse_field_to_subfield('300', 'e', 'extent', 'additional_content')

                    marc.parse_field_to_subfield('340', 'a', 'extent', 'carrier')

                    marc.parse_field('351', 'c', 'classification')

                    marc.parse_field_list(['600', '610', '611', '630', '648', '650', '651', '653', '655', '690', '691'],
                                          {'a': 'title', '2': 'source', '0': 'identifier'}, 'subject_headings')

                    marc.parse_field('856', 'u', 'link')
                    marc.parse_field_to_subfield('908', 'a', 'extent', 'format')
                    marc.parse_field('909', 'a', 'archive_tag')

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
                    if len(dsv05_dig.search(query=query)) > 0:
                        marc.add_value('is_digitized', True)
                    else:
                        marc.add_value('is_digitized', False)

                    if 'print_material' in marc.result:
                        if marc.result['print_material'] in ['Noten', 'Bildmaterial', 'Druckschrift', 'Kartenmaterial']:
                            marc.add_value_sub('final', 'type', 'print')
                        elif marc.result['print_material'] in ['Ton', 'Mikroform', 'Gegenstand', 'Filmmaterial']:
                            marc.add_value_sub('final', 'type', 'other')
                        elif marc.result['print_material'] in ['Manuskript', 'Notenmansukript']:
                            marc.add_value_sub('final', 'type', 'manuscript')
                        else:
                            marc.add_error_tag('_unknown_print_material')
                            logger.warning('Unknown print material: %s in %s.', marc.result['print_material'],
                                           marc.result['identifier'])
                    else:
                        marc.add_value_sub('final', 'type', 'manuscript')

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
                    elif date_tag_046 == 'ok':
                        year = marc.result['dates']['exact']['year']
                        marc.add_value_sub('final', 'year', int(year))
                        marc.append_value_sub('final', 'century', int(year / 100) + 1)
                    else:
                        marc.add_error_tag('_no_valid_date')

                    index.index_into(marc.result, marc.result['identifier'])
