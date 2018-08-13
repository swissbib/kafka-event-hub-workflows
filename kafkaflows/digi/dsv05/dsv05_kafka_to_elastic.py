from kafkaflows.digi.utility.mapper import MARCMapper
from kafkaflows.digi.utility.vufind_format_codes import swissbib_format_codes

from kafka_event_hub.consumers import ElasticConsumer
from simple_elastic import ElasticIndex
from pymarc.reader import JSONReader

import logging
import io
import re

dsv05_dig = ElasticIndex('dsv05-digitised', 'marc', url='http://sb-ues2.swissbib.unibas.ch:9200')
format_dict = swissbib_format_codes()


def parse_date(record: MARCMapper):
    _008_date = record.parse_date_from_008()
    _046_date = record.parse_date_from_046()
    if _008_date:
        year = record.result['dates']['date']['year']
        record.add_value_sub('final', 'year', int(year))
        record.append_value_sub('final', 'century', int(year / 100) + 1)
    elif _046_date:
        year = record.result['dates']['exact']['year']
        record.add_value_sub('final', 'year', int(year))
        record.append_value_sub('final', 'century', int(year / 100) + 1)
    elif record.parse_date_from_264():
        year = record.result['dates']['parsed_264_year']
        record.add_value_sub('final', 'year', int(year))
        record.append_value_sub('final', 'century', int(year / 100) + 1)
    else:
        record.add_value_sub('final', 'year', -1)
        record.add_error_tag('_no_valid_date')


def pre_filer(message: str) -> bool:
    if re.search('{"F": "(A100|A125|A130)"},', message):
        return False


def transformation(message: str) -> dict:
    reader = JSONReader(io.StringIO(message))
    for record in reader:
        marc = MARCMapper(record)

        for field in marc.get_fields('949'):
            marc.add_value('library', field['F'])
            marc.add_identifier('call_number', field['j'])

        marc.add_value('database', 'dsv05')

        marc.identifier()
        marc.add_identifier('dsv05', marc['001'].value()[3:])
        if record['024'] is not None:
            if record['024']['a'] is not None:
                marc.add_identifier('doi', record['024']['a'])

        marc.parse_leader()
        marc.parse_cat_date()

        parse_date(marc)

        marc.parse_rest_008()

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
                logging.warning('Unknown print material: %s in %s.', marc.result['print_material'],
                                 marc.result['identifier'])
        else:
            marc.add_value_sub('final', 'type', 'manuscript')

        if 'coverage' in marc.result['extent']:
            matches = re.findall('\d+', marc.result['extent']['coverage'])
            _sum = 0
            for match in matches:
                _sum += int(match)
            if re.search('Bd.|B[äa]nd[e]?|[Tt]omes|T((h)?eil)?.|[Vv]ol.|[Hh]eft(en)?',
                         marc.result['extent']['coverage']):
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

        if 'date' in marc.result['production']:
            marc.result['final']['display_date'] = marc.result['production']['date']

        return marc.result


def after_filer(transformed_message: dict) -> bool:
    if int(transformed_message['final']['year']) > 1920:
        return True


def update(old: dict, new: dict) -> dict:
    #TODO: Implement this to do something!
    return old


def run_dsv05_consumer():
    logger = logging.getLogger(__name__)
    consumer = ElasticConsumer('configs/dsv05/elastic_consumer.yml', logger)
    consumer.set_pre_filter_policy(pre_filer)
    consumer.set_transformation_policy(transformation)
    consumer.set_after_filter_policy(after_filer)
    consumer.set_update_policy(update)
    while True:
        consumer.consume()