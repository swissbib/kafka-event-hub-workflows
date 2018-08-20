from kafkaflows.digi.utility.mapper import MARCMapper
from kafkaflows.digi.utility.vufind_format_codes import swissbib_format_codes

from kafka_event_hub.consumers.utility import DataTransformation
from simple_elastic import ElasticIndex
from roman import fromRoman, InvalidRomanNumeralError

import typing
import logging
import re

format_dict = swissbib_format_codes()

find_roman_numeral = re.compile('([MCLXVI]+)[^a-z]')
roman_numeral = re.compile('^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')


class TransformSruExport(DataTransformation):

    def __init__(self, database, config, logger=logging.getLogger(__name__)):
        super().__init__(logger)
        self._database = database
        self._config = config
        self.marc = None
        self.digidata_index = ElasticIndex(**config['digidata'])
        self.swissbib_elk_host = config['swissbib.host']
        self.opac = ElasticIndex(**config['opac'])
        self.reservations = ElasticIndex(**config['reservations'])

    def transform(self, value: str) -> dict:
        self.marc = MARCMapper(value)
        self.marc.add_value('database', self._database)
        self.marc.identifier()

        if self.marc['024'] is not None:
            if self.marc['024']['a'] is not None:
                self.marc.add_identifier('doi', self.marc['024']['a'])

        self.marc.add_identifier('swissbib', self.marc['001'].value())

        if self._database == 'dsv01':
            for _035 in self.marc.get_fields('035'):
                if _035['a'] is not None:
                    if _035['a'].startswith('(IDSBB)'):
                        self.marc.add_identifier('dsv01', _035['a'].split(')')[1])
        elif self._database == 'dsv05':
            self.marc.add_identifier('dsv05', self.marc['001'].value()[3:])

        # Do not re-order these!
        self.enrich()

        self.parse_record_type()
        self.parse_date()
        self.parse_format_codes()
        self.parse_number_of_pages()
        self.parse_call_number()

        self.parse_additional_information()

        return self.marc.result

    def enrich(self):
        """Enriching the message from other data sources."""
        self.enrich_digidata()
        self.enrich_swissbib_hits()
        self.enrich_opac_hits()
        self.enrich_loans_and_reservations()

    def enrich_digidata(self):
        """Loads data from the digidata elastic repository.

        No live updates, as the digidata repository is on Afrikaportal-elastic,
        which is only on localhost accessible. To update run the digispace-producer & digispace-consumer.

        TODO: Load live data instead of copy.
        """
        query = {
            "query": {
                "term": {
                    "system_number": self.marc.result['identifiers'][self._database]}
            }
        }
        result = self.digidata_index.search(query=query)
        if len(result) > 0:
            self.marc.add_value('is_digitized', True)
            if 'number_of_images' in result[0]:
                self.marc.add_value('number_of_images', result[0]['number_of_images'])
        else:
            self.marc.add_value('is_digitized', False)

    def enrich_swissbib_hits(self):
        """Add the hits from swissbib on a yearly basis.

        TODO: Maybe load by month?
        """
        identifier = self.marc.result['identifier']

        hits = dict()
        for year in range(2017, 2019):
            sru = ElasticIndex('sru-{}'.format(year), doc_type='logs',
                               url=self.swissbib_elk_host)
            hits['sru'] = dict()
            query = {'query': {'match': {'requestparams': identifier}}}
            hits['sru'][str(year)] = len(sru.scan_index(query=query))

        for source in ['green', 'jus', 'bb']:
            hits[source] = dict()
            for year in range(2017, 2019):
                swissbib = ElasticIndex('swissbib-{}-{}'.format(source, year),
                                        doc_type='logs',
                                        url=self.swissbib_elk_host)

                query = {'query': {'term': {'request_middle.keyword': {'value': identifier}}}}
                hits[source][str(year)] = len(swissbib.scan_index(query=query))

        self.marc.result['hits'] = hits

    def enrich_opac_hits(self):
        """Adds opac-access hits to data.

        IMPORTANT: The opac messages do not distinguish between dsv01 and dsv05 system numbers.
        This makes it impossible to figure out which one it is.

        IMPLEMENTED WORKAROUND:
        - If the system number is higher than 320'000 it is dsv01
        - If the system number is blow 320'000 it is more likely in dsv05
        As such opac its below 320'000 are always attributed to dsv05

        TODO: Implement a better distinction between databases.
        TODO: Implement opac access numbers per year/month
        """
        query = {
            'query': {
                'term': {
                    'system_number': {
                        'value': self.marc.result['identifiers'][self._database]
                    }
                }
            }
        }
        hits = len(self.opac.scan_index(query=query))
        identifier = int(self.marc.result['identifiers'][self._database])
        if identifier > 320000 and self._database == 'dsv01':
            self.marc.add_value('opac_access', hits)
        elif identifier <= 320000 and self._database == 'dsv05':
            self.marc.add_value('opac_access', hits)
        elif identifier <= 320000 and self._database == 'dsv01':
            self.marc.add_value('opac_access', hits)
            self.marc.add_error_tag('_maybe_dsv05_hits')

    def enrich_loans_and_reservations(self):
        """Gets the number of loans and reservations from an elastic index.

        Only used for dsv01 currently as dsv05 data is not available.

        TODO: Get dsv05 data.
        """
        if self._database == 'dsv01':
            query = {
                'query': {
                    'term': {
                        'system_number': {
                            'value': self.marc.result['identifiers'][self._database]
                        }
                    }
                }
            }
            results = self.reservations.scan_index(query=query)
            for record in results:
                if 'reservations' in record:
                    self.marc.add_value_sub(record['year'], 'reservations', record['reservations'])
                if 'loans' in record:
                    self.marc.add_value_sub(record['year'], 'loans', record['loans'])

    def parse_date(self):
        """Parsing the date from the various possible fields. Stores where the information was taken from."""
        _008_date = self.marc.parse_date_from_008()
        _046_date = self.marc.parse_date_from_046()
        if _008_date:
            year = self.marc.result['dates']['date']['year']
            self.marc.add_value_sub('final', 'year', int(year))
            self.marc.append_value_sub('final', 'century', int(year / 100) + 1)
            self.marc.add_value_sub('source', 'year', '008')
        elif _046_date:
            year = self.marc.result['dates']['exact']['year']
            self.marc.add_value_sub('final', 'year', int(year))
            self.marc.append_value_sub('final', 'century', int(year / 100) + 1)
            self.marc.add_value_sub('source', 'year', '046')
        elif self.marc.parse_date_from_264():
            year = self.marc.result['dates']['parsed_264_year']
            self.marc.add_value_sub('final', 'year', int(year))
            self.marc.append_value_sub('final', 'century', int(year / 100) + 1)
            self.marc.add_value_sub('source', 'year', '264')
        else:
            self.marc.add_value_sub('source', 'year', 'None')
            self.marc.add_error_tag('_no_valid_date')

    def parse_number_of_pages(self):
        """Figure out the number of pages!

        First source: digidata number of images.
        Second source: coverage
        Third source: estimates.

        TODO: Improve this a lot!
        """
        pages = 0

        if 'number_of_pages' in self.marc.result:
            pages = self.marc.result['number_of_pages']
        else:
            self.marc.parse_field_to_subfield('300', 'a', 'extent', 'coverage')
            num = 0
            name = 'None'
            if 'c-format' in self.marc.result:
                if self.marc.result['c-format'] in ['Schallplatte', 'Diverse Filmformate', 'Diverse Tonformate']:
                    num = 1
                    name = 'Gegenstand'
                if self.marc.result['c-format'] in ['Datenbank', 'Zeitung']:
                    num = 0
                    name = 'Periodikum'

            if name == 'None':
                if 'coverage' in self.marc.result['extent']:
                    num, name = self.parse_coverage_field()

            if name == 'Seiten':
                pages = num

        if pages == 0:
            self.marc.add_error_tag('_no_page_value')
            # TODO: Add estimates!
        else:
            self.marc.add_value_sub('final', 'pages', pages)

    def parse_coverage_field(self) -> typing.Tuple[float, str]:
        """Parses various values from the coverage field and returns them as tuple:

        (number of unit, name of unit)

        Possible units are:

        Seiten

        Laufmeter = 8'000 Seiten

        Band = 245 Seiten
        Dossier

        Partitur = 50 Seiten

        Gegenstand

        Anderes

        None
        """
        coverage = self.marc.result['extent']['coverage']
        swisbib_format = self.marc.result['c-format']

        if swisbib_format == 'Atlas':
            pages = 0
            match = re.findall('(\d+) (Taf|Kt|S(eiten)?|(Titel)?[Bb]l(ätter(n)?)?|Kart(e(n)?)?)', coverage)
            if len(match) > 0:
                for l in match:
                    pages += int(l[0])

            if pages > 0:
                return pages, 'Seiten'

        if swisbib_format in ['Klavierauszug']:
            pages = 0
            match = re.search('(\d+) (S|p)', coverage)
            if match:
                pages = int(match.group(1))

            if pages > 0:
                return pages, 'Seiten'
            else:
                # TODO: Nachfragen ob Partituren eine eigene Seitenzahl erhalten sollten.
                # 50 Seiten
                return 1, 'Partitur'

        if swisbib_format in ['Partitur']:
            pages = 0
            match = re.search('(\d+) (S(eiten)?|p|Bl(ätter)?)', coverage)
            if match:
                pages += int(match.group(1))

            match = find_roman_numeral.search(coverage)
            if match:
                roman_number = roman_numeral.fullmatch(match.group(1))
                if roman_number:
                    pages += fromRoman(roman_number.group(0))
            if pages > 0:
                return pages, 'Seiten'
            else:
                band = 0
                match = re.search('(\d+) (Abt|B|C|H[^y]|He|K|[Pp]art|Ser|T|[Vv]ol)', coverage)
                if match:
                    band += int(match.group(1))
                # TODO: Document Partitur
                # average 50 pages
                if band > 0:
                    return band, 'Partitur'
                else:
                    return 1, 'Partitur'

        # No useful coverage value.
        # ca. 140'000 records.
        if re.match('\s+v\.$', coverage):
            return 0, 'None'

        # Simple number of pages:
        match_pages = re.fullmatch('(\[)?(?P<number>[0-9]+)(\])? ([Ss](eiten|.)?|p(ages)?)', coverage)
        if match_pages:
            return int(match_pages.groupdict()['number']), 'Seiten'

        # Number of pages with roman numeral:
        # will ignore roman numerals which are not valid.
        match_pages_roman = re.fullmatch('(?P<roman>[CVXILMcvixlm]+)[,.] (?P<number>[0-9]+) ([Ss](eiten|.)?|p(ages)?)$', coverage)
        if match_pages_roman:
            result = match_pages_roman.groupdict()
            try:
                roman = fromRoman(result['roman'])
            except InvalidRomanNumeralError:
                pages = int(result['number'])
            else:
                pages = roman + int(result['number'])
            return pages, 'Seiten'

        # Laufmeter
        match_lfm = re.fullmatch('([Cc]a\. )?(?P<number>[0-9]+,[0-9]+) (m|Lfm|Laufmeter|lfd.m)( \(.*\))?', coverage)
        if match_lfm:
            # 1 Laufmeter = 8'000 Seiten
            return float(match_lfm.groupdict()['number'].replace(',', '.')), 'Laufmeter'

        # Postcards
        # X Bl. ; A4/A5/X cm
        match_postcard = re.fullmatch('(?P<number>[0-9]+) Bl\. ; '
                                      '((?P<format>[ ]?[456])|(?P<size>[0-9]+)[ ]?cm)', coverage)
        if match_postcard:
            return int(match_postcard.groupdict()['number']), 'Seiten'

        # Fotos
        match = re.search('(\d+) (\w+ )?((Ph|F)oto|Repro)', coverage)
        if match:
            return int(match.group(1)), 'Seiten'
            # 80 Seiten Band

        # Find letters!
        letters = re.search('Brief[e]?', coverage)

        if letters:
            # Select pages or letters. Both are counted as a page each.
            # Fairly accurate as long as the source is right...
            letter_numbers = re.findall('([0-9]+) (\w+)[ ]?(\(([0-9]+) (\w+)\))?', coverage)

            if len(letter_numbers) > 0:
                pages = 0
                for l in letter_numbers:
                    if l[4] == '' and l[1] in ['Briefe', 'Brief', 'Briefen',
                                               'Antwortbriefe', 'Antwortbrief', 'Einzelbriefe', 'Briefwechsel',
                                               'Gegenbriefe', 'Gegenbrief',
                                               'Karte', 'Karten', 'Zeitungsausschnitt',
                                               'Postkarte', 'Postkarten',
                                               'Telegramme', 'Telegramm', 'Kurznachricht',
                                               'Ansichtskarten', 'Ansichtskarte', 'Blatt',
                                               'Kärtchen', 'Briefkarte', 'Aerogarmm', 'Manuskripte', 'Gefalteter',
                                               'Doppelkarten', 'Dokumente', 'Grundrisse', 'Zettel',
                                               'Neujahreskarten', 'weiterer', 'numerierte', 'Zeitungsauschnitt',
                                               'Zeugnis', 'Neujahrskarten', 'Stück', 'Briefkarten',
                                               'Bl', 'S', 'Fotonegative', 'Fotopositive', 'Artikeln'
                                               ]:
                        pages += int(l[0])
                    elif l[1] in ['Couvert', 'Schachtel', 'Band'] and l[4] in ['Briefe']:
                        pages += int(l[3]) * 3
                    elif l[4] in ['Blatt', 'Bl']:
                        pages += int(l[3])
                if pages > 0:
                    return pages, 'Seiten'
                else:
                    # Briefe pro Mappe (ca. 40 Seiten)
                    # average assumptions.
                    return 3, 'Seiten'
            # END LETTERS

        # part pages
        half_pages = re.fullmatch('([0-9]+)([½¾]|[.,][0-9]+| [0-9]/[0-9]) (Bl|S)\.', coverage)
        if half_pages:
            pages = int(half_pages.group(1)) + 1
            if pages > 0:
                return pages, 'Seiten'

        # Schachteln = 800 Seiten
        # Mappen = 80 Seiten

        return 0, 'None'

    def parse_record_type(self):
        """Defines a general type for the record.

        This is used to distinguish between prints and hand written manuscripts.
        """
        self.marc.parse_field('245', 'h', 'print_material')

        if self._database == 'dsv01':
            self.marc.add_value_sub('final', 'type', 'print')
        elif 'print_material' in self.marc.result:
            if self.marc.result['print_material'] in ['Noten', 'Bildmaterial', 'Druckschrift', 'Kartenmaterial']:
                self.marc.add_value_sub('final', 'type', 'print')
            elif self.marc.result['print_material'] in ['Ton', 'Mikroform', 'Gegenstand', 'Filmmaterial']:
                self.marc.add_value_sub('final', 'type', 'other')
            elif self.marc.result['print_material'] in ['Manuskript', 'Notenmanuskript']:
                self.marc.add_value_sub('final', 'type', 'manuscript')
            else:
                self.marc.add_error_tag('_unknown_print_material')
                logging.warning('Unknown print material: %s in %s.', self.marc.result['print_material'],
                                self.marc.result['identifier'])
        else:
            self.marc.add_value_sub('final', 'type', 'manuscript')

    def parse_call_number(self):
        """Parses the call number of this record has.

        Adds the library it belongs to as well. The call number is further
        indexed in parts to create facets.

        Only books from A100 & A125 are used.

        Books older than 1920 are very rare in A140 (UB Medizin)
        The books in A130 (Altertum) are ignored, because there are not that many, and it would
        be necessary to further filter the books from UBH.
        """
        for field in self.marc.get_fields('949'):

            if field['F'] in ['A100', 'A125']:
                self.marc.add_value('library', field['F'])
                self.marc.append_value_sub('exemplar', 'call_number', field['j'])
                if field['s'] is not None:
                    self.marc.append_value_sub('exemplar', 'secondary_call_number', field['s'])

        # TODO: Implement a way to process all the call numbers, since one title
        # can have many of them.
        # currently just picks the first one.
        # books can have multiple call numbers for two reasons:
        # 1. The library owns more than one item.
        # 2. The bibliographic record describes multiple parts of one title.
        if 'call_number' in self.marc.result['exemplar']:
            call_number = self.marc.result['exemplar']['call_number'][0]
            if ':' in call_number:
                call_number_part, volume_number = call_number.split(':')
                volume_number = volume_number.strip()
                values = call_number_part.split(' ')
            else:
                volume_number = None
                values = call_number.split(' ')
            self.marc.add_value_sub('filter', 'prefix', values[0])
            count = 0
            for value in values[1:]:
                count += 1
                self.marc.add_value_sub('filter', 'part-{}'.format(count), value)

            if volume_number:
                self.marc.add_value_sub('filter', 'volume', volume_number)

    def parse_format_codes(self):
        """Parse the format codes and replace them with human readable forms.

        The c-format, the most condensed value is used as format.
        """
        self.marc.parse_field('898', 'a', 'a-format')
        if 'a-format' in self.marc.result:
            self.marc.result['a-format'] = format_dict[self.marc.result['a-format']]
        self.marc.parse_field('898', 'b', 'b-format')
        if 'b-format' in self.marc.result:
            self.marc.result['b-format'] = format_dict[self.marc.result['b-format']]
        self.marc.parse_field('898', 'c', 'c-format')
        if 'c-format' in self.marc.result:
            self.marc.result['c-format'] = format_dict[self.marc.result['c-format']]
            self.marc.add_value_sub('final', 'format', self.marc.result['c-format'])

    def parse_additional_information(self):
        """Information which might be interesting in the future, but not needed for current analysis."""
        self.marc.parse_leader()

        self.marc.parse_cat_date()

        self.marc.parse_rest_008()

        self.marc.parse_field('245', 'a', 'title')
        self.marc.parse_field('245', 'b', 'subtitle')
        self.marc.parse_field('245', 'c', 'author')

        self.marc.parse_field_to_subfield('264', 'a', 'production', 'place')
        self.marc.parse_field_to_subfield('264', 'b', 'production', 'publisher')
        self.marc.parse_field_to_subfield('264', 'c', 'production', 'date')

        self.marc.parse_field_to_subfield('300', 'b', 'extent', 'physical_attributes')
        self.marc.parse_field_to_subfield('300', 'c', 'extent', 'size_and_format')
        self.marc.parse_field_to_subfield('300', 'e', 'extent', 'additional_content')

        self.marc.parse_field_append_to_subfield('336', 'a', 'extent', 'content')
        self.marc.parse_field_append_to_subfield('337', 'a', 'extent', 'media')
        self.marc.parse_field_append_to_subfield('338', 'a', 'extent', 'carrier')
        self.marc.parse_field_to_subfield('348', 'a', 'extent', 'music')

        self.marc.parse_field('351', 'c', 'classification')

        self.marc.parse_field('250', 'a', 'version')

        self.marc.parse_field_to_subfield('340', 'a', 'extent', 'carrier')

        self.marc.parse_field_list(['600', '610', '611', '630', '648', '650', '651', '653', '655', '690', '691'],
                                   {'a': 'title', '2': 'source', '0': 'identifier'},
                                   'subject_headings')

        self.marc.parse_field('856', 'u', 'link')
        self.marc.parse_field_to_subfield('908', 'a', 'extent', 'format')
        self.marc.parse_field('909', 'a', 'archive_tag')

        if 'date' in self.marc.result['production']:
            self.marc.result['final']['display_date'] = self.marc.result['production']['date']

    def pre_filter(self, message: str) -> bool:
        """Keep only records which belong to Universitätsbibliothek Basel."""
        if re.search('{"F": "(A100|A125)"},', message):
            return False
        else:
            return True

    def post_filter(self, transformed_message: dict) -> bool:
        # Remove any record which is newer than 1920.
        if 'year' in transformed_message['final']:
            if int(transformed_message['final']['year']) > 1920:
                return True

        # Remove records of special formats.
        if transformed_message['final']['format'] in ['Objekt',
                                                      'Diverse Tonformate',
                                                      'Schallplatte',
                                                      'Diverse Filmformate',
                                                      'Datenbank']:
            return True

        return False



