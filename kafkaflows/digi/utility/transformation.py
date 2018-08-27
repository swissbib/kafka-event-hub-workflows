from kafkaflows.digi.utility.mapper import MARCMapper
from kafkaflows.digi.utility.vufind_format_codes import swissbib_format_codes
from kafkaflows.digi.e_plattforms.collect_hits import get_vlids, get_mapping_vlids_sys_num

from kafka_event_hub.consumers.utility import DataTransformation
from simple_elastic import ElasticIndex
from roman import fromRoman

from typing import Optional, Tuple, Union
import logging
import json
import re
from enum import Enum


class Units(Enum):
    No = 'None'
    Seiten = 'Seiten'

    Band = 'Band'
    Artikel = 'Artikel'

    # Musik
    Partitur = 'Partitur'
    Stimmen = 'Stimmen'

    # Handschriften
    Manuskriptband = 'Manuskriptband'
    Faszikel = 'Faszikel'

    # Karten
    Karten = 'Karten'
    Kartenmappen = 'Kartenmappen'

    # Archiv
    Laufmeter = 'Laufmeter'
    # Formattyp Dossier: Dossier[s], Stück, Serie
    Archiveinheit = 'Archiveinheit'
    Schachteln = 'Schachteln'
    Mappen = 'Mappen'

    # Briefe
    Briefe = 'Briefe'
    Briefband = 'Briefband'
    Briefmappen = 'Briefmappen'

    # Bilder
    Fotomappen = 'Fotomappen'

    Periodikum = 'Periodikum'
    Gegenstand = 'Gegenstand'


format_dict = swissbib_format_codes()

find_roman_numeral = re.compile('([MCLXVI]+)[^a-z]')
roman_numeral = re.compile('^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')

empty = re.compile('\s+v\.')

regex_pages = re.compile('[\[]?(\d+)[\]]? '
                         '('
                         '[Ss]([^a-z]|$)|'
                         '[Pp]([^a-z]|$)|'
                         '[Ss]eite[n]?|'
                         '[Pp]age[s]?|'
                         'Bl([äa]tt(er)?)?|'
                         'B[öo]gen|'
                         'Doppelblatt|'
                         '[Dd]o[kc]ument[es]?|'
                         'Manuskript|'
                         'fol|'
                         'Zettel[n]?'
                         ')')

regex_pages_single = re.compile('(S|Bl)[.]? (\d+)([^-0-9]|$)')
regex_pages_range = re.compile('(S|Bl|fol)[.]? (\d+)-(\d+)')
regex_pages_partial = re.compile('(\d+)?([ ]?[¼½¾]|[.,]\d+| \d/\d) (Bl|S)\.')

regex_volumes_word_number = re.compile('([Tt]([h]?eil[e]?)?|'
                                       'B[äa]nd[e]?|'
                                       'Bd[e]?|'
                                       '[Vv]ol(ume)?[s]?|'
                                       'H(eft)?(e|chen)?|'
                                       '[Tt]om[ei]?[s]?|'
                                       'N[or])[.]? [\[]?(\d+)[\]]?([^-]|$)')
regex_volumes_number_word = re.compile('[\[]?(\d+)[\]]? ([Tt]([h]?eil[e]?)?|'
                                       'B[äa]nd[e]?|'
                                       'Bd[e]?|'
                                       '[Vv]ol(ume)?[s]?|'
                                       'H(eft)?(e|chen)?|'
                                       '[Tt]om[ei]?[s]?|'
                                       'N[or])([^a-z]|$)')
regex_volumes_word_range = re.compile('([Tt]([h]?eil[e]?)?|'
                                      'B[äa]nd[e]?|'
                                      'Bd[e]?|'
                                      '[Vv]ol(ume)?[s]?|'
                                      'H(eft)?(e|chen)?|'
                                      '[Tt]om[ei]?[s]?|'
                                      'N[or])[.]? (\d+)-(\d+)')


regex_letters = re.compile('(\d+) ((Antwort|Gegen)?[bB]rief[e]?|Couvert[s]?)')
regex_boxes = re.compile('(\d+) (Archiv)?[Ss]chachtel(\(n\)|n)?( \((\d,\d+) m\))?')
regex_folders = re.compile('(\d+) Mappe(\(n\)|n)?')
regex_laufmeter = re.compile('(\d+(,\d+)?) (m|[Ll]fm|Laufmeter|lfd\.m)')

regex_piece = re.compile('(\d+) Stück')
regex_dossier = re.compile('(\d+) Dossier[s]?')
regex_serie = re.compile('(\d+) Serie')


def parse_archive(coverage: str, return_type: Units) -> Tuple[int, Units]:
    archive = 0
    results = regex_dossier.findall(coverage)
    for result in results:
        archive += int(result[0])

    results = regex_piece.findall(coverage)
    for result in results:
        archive += int(result[0])

    results = regex_serie.findall(coverage)
    for result in results:
        archive += int(result[0])

    if archive > 0:
        return archive, return_type
    else:
        return 0, Units.No


def parse_meters(coverage: str) -> Tuple[float, Units]:
    lfm = 0
    results = regex_laufmeter.findall(coverage)
    for result in results:
        lfm += float(result[0].replace(',', '.'))

    if lfm > 0:
        return lfm, Units.Laufmeter
    else:
        return 0, Units.No


def parse_folders(coverage: str, return_type: Units) -> Tuple[int, Units]:
    folders = 0
    results = regex_folders.findall(coverage)
    for result in results:
        folders += int(result[0])

    if folders > 0:
        return folders, return_type
    else:
        return 0, Units.No


def parse_boxes(coverage: str) -> Tuple[Union[float, int], Units]:
    boxes = lfm = 0
    results = regex_boxes.findall(coverage)
    for result in results:
        if result[3] == '':
            boxes += int(result[0])
        else:
            lfm += float(result[4].replace(',', '.'))

    if boxes > 0:
        return boxes, Units.Schachteln
    elif lfm > 0:
        return lfm, Units.Laufmeter
    else:
        return 0, Units.No


def parse_letters(coverage: str) -> Tuple[int, Units]:
    letters = 0
    results = regex_letters.findall(coverage)
    for result in results:
        letters += int(result[0])

    if letters > 0:
        return letters, Units.Briefe
    else:
        return 0, Units.No


def parse_volumes(coverage: str, return_type: Units) -> Tuple[int, Units]:
    volumes = 0
    results = regex_volumes_word_number.findall(coverage)
    if len(results) > 0:
        volumes += 1

    results = regex_volumes_number_word.findall(coverage)
    for result in results:
        volumes += int(result[0])

    results = regex_volumes_word_range.findall(coverage)
    for result in results:
        volumes += int(result[6]) - int(result[5]) + 1

    if volumes > 0:
        return volumes, return_type
    else:
        return 0, Units.No


def parse_pages(coverage: str) -> Tuple[int, Units]:
    pages = 0
    results = regex_pages.findall(coverage)
    for result in results:
        pages += int(result[0])

    results = regex_pages_range.findall(coverage)
    for result in results:
        pages += int(result[2]) - int(result[1]) + 1

    results = regex_pages_single.findall(coverage)
    for result in results:
        pages += int(result[1])

    results = regex_pages_partial.findall(coverage)
    for result in results:
        if result[0] == '':
            pages = 1
        else:
            pages += int(result[0]) + 1

    if pages > 0:
        result = find_roman_numeral.search(coverage)
        if result:
            roman_number = roman_numeral.fullmatch(result.group(1))
            if roman_number:
                pages += fromRoman(roman_number.group(0))

    if pages > 0:
        return pages, Units.Seiten
    else:
        return 0, Units.No


class TransformSruExport(DataTransformation):

    def __init__(self, database, config, logger=logging.getLogger(__name__)):
        super().__init__(logger)
        self._database = database
        self._config = config
        self.marc = None
        self.digidata_index = ElasticIndex(**config['digidata'])
        self.swissbib_elk_host = config['swissbib.host']
        with open(config['e-plattforms'], 'r') as fp:
            self.e_plattform_data = json.load(fp)
        self.opac = ElasticIndex(**config['opac'])
        self.reservations = ElasticIndex(**config['reservations'])
        self.page_conversion_rates = config['page-conversions']

    def transform(self, value: str) -> dict:
        # Do not reoder this function!
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
        self.enrich_e_plattform_data()

    def enrich_digidata(self):
        """Loads data from the digidata elastic repository.

        No live updates, as the digidata repository is on Afrikaportal-elastic,
        which is only on localhost accessible. To update run the digispace-producer & digispace-consumer.

        TODO: Load live data instead of copy. To do this direct access to Afrikaportal is necessary.
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
            total_reservations = 0
            total_loans = 0
            for record in results:
                if 'reservations' in record:
                    total_reservations += record['reservations']
                    self.marc.add_value_sub('reservations', record['year'], record['reservations'])
                if 'loans' in record:
                    total_loans += record['loans']
                    self.marc.add_value_sub('loans', record['year'], record['loans'])
            self.marc.add_value_sub('reservations', 'total', total_reservations)
            self.marc.add_value_sub('loans', 'total', total_loans)
        else:
            # place holder values for scripted fields.
            self.marc.add_value_sub('reservations', 'total', 0)
            self.marc.add_value_sub('loans', 'total', 0)

    def enrich_e_plattform_data(self):
        """Enrich the collected hits from e-plattforms (e-rara & e-manuscripta)."""
        identifier = self.marc.result['identifiers'][self._database]
        if identifier in self.e_plattform_data:
            data = self.e_plattform_data[identifier]
            data['total'] = data['2016'] + data['2017'] + data['2018']
            self.marc.add_value_sub('hits', 'e-plattform', data)
        else:
            self.marc.add_value_sub('hits', 'e-plattform', {'2016': 0, '2017': 0, '2018': 0, 'total': 0})


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
        """

        self.marc.parse_field_to_subfield('300', 'a', 'extent', 'coverage')
        pages = 0
        name = Units.No
        # This will be filtered anyway.
        if self.marc.result['c-format'] in ['Objekt', 'Diverse Tonformate', 'Schallplatte', 'Diverse Filmformate', 'Datenbank']:
            pages = 1
            name = Units.Gegenstand
            self.marc.add_value_sub('source', 'pages', 'format')

        if name == Units.No:
            pages, name = self.parse_coverage_field()

            if name == Units.No:
                raise ValueError('Name should not be None here: {}. {}'.format(self.marc.result['identifier'], pages))

            if name != Units.Seiten:
                self.marc.add_value_sub('source', 'pages', 'estimate')
                self.marc.add_value_sub('source', 'estimate', name.value)
                pages = pages * self.page_conversion_rates[name.value]
            else:
                self.marc.add_value_sub('source', 'pages', 'coverage')

            self.marc.add_value_sub('extent', 'pages', pages)

        if 'number_of_images' in self.marc.result:
            pages = self.marc.result['number_of_images']
            self.marc.add_value_sub('source', 'pages', 'digidata')
            if 'estimate' in self.marc.result['source']:
                del self.marc.result['source']['estimate']

        self.marc.add_value_sub('final', 'pages', pages)

    def parse_coverage_field(self) -> Tuple[Union[float, int], Units]:
        """Parses various values from the coverage field and returns them as tuple:

        (number of unit, name of unit)
        """
        if 'coverage' in self.marc.result['extent']:
            coverage = self.marc.result['extent']['coverage']
        else:
            coverage = None
        swissbib_format = self.marc.result['c-format']

        if swissbib_format in ['Klavierauszug', 'Partitur', 'Noten']:
            return self.parse_partituren(coverage)
        elif swissbib_format in ['Atlas', 'Karte', 'Diverse Kartenformate']:
            return self.parse_maps(coverage)
        elif swissbib_format in ['Brief', 'Briefsammlung']:
            return self.parse_letters(coverage)
        elif swissbib_format in ['Diverse Bildformate', 'Fotografie']:
            return self.parse_fotos(coverage)
        elif swissbib_format in ['Gesamtwerk', 'Buch', 'Verfassung / Gesetz', 'Artikel']:
            return self.parse_books(coverage, swissbib_format)
        elif swissbib_format in ['Handschrift']:
            return self.parse_manuscript(coverage)
        elif swissbib_format in ['Dossier']:
            return self.parse_dossier(coverage)
        elif swissbib_format in ['Zeitung', 'Zeitschrift / Schriftenreihe']:
            # TODO: Bessere implementierung von Zeitschriften.
            if coverage is None:
                return 1, Units.Periodikum

            num, name = parse_volumes(coverage, Units.Band)
            if num > 0:
                return num, name

            year = None
            to = None
            if 'dates' in self.marc.result:
                if 'date' in self.marc.result['dates']:
                    if 'year' in self.marc.result['dates']['date']:
                        year = self.marc.result['dates']['date']['year']
                    if 'to' in self.marc.result['dates']['date']:
                        to = self.marc.result['dates']['date']['to']

            if year is not None and to is not None:
                return year - to, Units.Band
            elif year is not None:
                return 1, Units.Band
            else:
                return 1, Units.Periodikum
        else:
            logging.error('Could not parse %s, with coverage %s and format %s.', self.marc.result['identifier'],
                          coverage, swissbib_format
                          )
            return 1, Units.Seiten

    def parse_partituren(self, coverage: str) -> Tuple[Union[float, int], Units]:
        if coverage is None or empty.fullmatch(coverage):
            return 1, Units.Partitur

        num, name = parse_pages(coverage)
        if num > 0:
            return num, name

        stimmen = re.match('Stimme', coverage)
        if stimmen:
            return 1, Units.Stimmen

        stimmen = re.match('Stimmen', coverage)
        if stimmen:
            return 3, Units.Stimmen

        num, name = parse_volumes(coverage, Units.Partitur)

        results = re.findall('(\d+) Stimme[n]', coverage)
        for result in results:
            num += int(result[0]) / 2

        results = re.findall('(\d+) (Abt|B|C|H$|He|K|[Pp]art|Ser|T|[Vv]ol)', coverage)
        for result in results:
            num += int(result[0])

        if num > 0:
            return num, Units.Partitur

        num, name = parse_meters(coverage)
        if num > 0:
            return num, name

        return 1, Units.Partitur

    def parse_maps(self, coverage: str) -> Tuple[Union[float, int], Units]:
        if coverage is None or empty.fullmatch(coverage):
            return 4, Units.Karten

        num, name = parse_pages(coverage)
        if name == 'Seiten':
            return num, name

        maps_matches = re.findall('(\d+) ([Kc]arte[n]?|Pl[äa]n[e]?|Vogel|Ansicht|Panorama|Manuskript)', coverage)
        maps = 0
        for matches in maps_matches:
            maps += int(matches[0])
        if maps > 0:
            return maps, Units.Karten

        atlas_matches = re.findall('(\d+) (Atlas)', coverage)

        atlas = 0
        for match in atlas_matches:
            atlas += int(match[0])
        if atlas > 0:
            return atlas, Units.Band
        folders, name = parse_folders(coverage, Units.Kartenmappen)
        if folders > 0:
            return folders, name

        return 4, Units.Karten

    def parse_letters(self, coverage: str) -> Tuple[Union[float, int], Units]:
        if coverage is None or empty.fullmatch(coverage):
            return 2, Units.Briefe

        pages, name = parse_pages(coverage)

        results = re.findall('(\d+) (Karte|Briefkarte|Postkarte|Ansichtskarte|Visitenkarte)', coverage)
        for result in results:
            pages += int(result[0])

        result = re.match('Briefkarte|Postkarte|Zettel|Karte|Visitenkarte', coverage)
        if result:
            pages += 1

        if pages > 0:
            return pages, Units.Seiten

        letters, name = parse_letters(coverage)
        if letters > 0:
            return letters, name

        volumes, name = parse_volumes(coverage, Units.Briefband)
        if volumes > 0:
            return volumes, name

        folders, name = parse_folders(coverage, Units.Briefmappen)
        if folders > 0:
            return folders, name

        return 2, Units.Briefe

    def parse_fotos(self, coverage: str) -> Tuple[int, Units]:
        if coverage is None or empty.fullmatch(coverage):
            return 1, Units.Seiten

        pages, name = parse_pages(coverage)

        results = re.findall('(\d+) (Kupferstich|Litho|Foto|Zeichnung|Repro|Holzschnitt|Schattenriss'
                             '|Aquarell|Druckgrafik(en)?|Physionotrace|Bild|Stück|Radierung)', coverage)
        for result in results:
            pages += int(result[0])

        if pages > 0:
            return pages, Units.Seiten

        folders, name = parse_folders(coverage, Units.Fotomappen)
        if folders > 0:
            return folders, name

        return 1, Units.Seiten

    def parse_books(self, coverage: str, swissbib_format: str) -> Tuple[int, Units]:
        if swissbib_format == 'Artikel':
            return_type = Units.Artikel
        else:
            return_type = Units.Band

        if coverage is None or empty.fullmatch(coverage):
            return 1, return_type

        num, name = parse_pages(coverage)
        if num > 0:
            return num, name

        volumes, name = parse_volumes(coverage, return_type)
        if volumes > 0:
            return volumes, name

        return 1, return_type

    def parse_manuscript(self, coverage: str) -> Tuple[Union[float, int], Units]:
        if coverage is None or empty.fullmatch(coverage):
            return 1, Units.Faszikel

        num, name = parse_pages(coverage)
        if num > 0:
            return num, name

        volumes, name = parse_volumes(coverage, Units.Manuskriptband)
        if volumes > 0:
            return volumes, name

        folders, name = parse_folders(coverage, Units.Faszikel)

        results = re.findall('(\d+) (Stücke|Papiertüte[n]?|Faszikel|Dossier|Broschüre|Zeichenbuch|'
                             'Heft(e|chen)?|Schuber|Bündel|Konvolut|Schulheft|Umschläge|Büchlein|Umschlag|Predigten)',
                             coverage)
        for result in results:
            volumes += int(result[0])

        if folders > 0:
            return folders, Units.Faszikel

        num, name = parse_boxes(coverage)

        if num > 0:
            return num, name

        letters, name = parse_letters(coverage)

        if letters > 0:
            return letters, name

        return 1, Units.Faszikel

    def parse_dossier(self, coverage: str) -> Tuple[Union[int, float], Units]:
        if coverage is None or empty.fullmatch(coverage):
            return 1, Units.Archiveinheit

        pages, name = parse_pages(coverage)
        if pages > 0:
            return pages, name

        volumes, name = parse_volumes(coverage, Units.Band)
        if volumes > 0:
            return volumes, name

        boxes, name = parse_boxes(coverage)
        if boxes > 0:
            return boxes, name

        folders, name = parse_folders(coverage, Units.Mappen)
        if folders > 0:
            return folders, name

        lfm, name = parse_meters(coverage)
        if lfm > 0:
            return lfm, name

        letters, name = parse_letters(coverage)
        if letters > 0:
            return letters, name

        archives, name = parse_archive(coverage, Units.Archiveinheit)
        if archives > 0:
            return archives, name

        return 1, Units.Archiveinheit

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
                self.marc.add_value_sub('final', 'type', 'other')
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

        # TODO: Implement a way to process all the call numbers, since one title
        # can have many of them.
        # currently just picks the first one.
        # books can have multiple call numbers for two reasons:
        # 1. The library owns more than one item.
        # 2. The bibliographic record describes multiple parts of one title.
        """
        for field in self.marc.get_fields('949'):

            if field['F'] in ['A100', 'A125']:
                self.marc.append_value('library', field['F'])
                if field['j'] != '':
                    self.marc.append_value('call_number', field['j'])

        if 'call_number' in self.marc.result:
            results = self.create_call_number_filter()
            if results is not None:
                self.marc.add_value_sub('filter', 'prefix', results[0])
                if results[1] is not None:
                    self.marc.add_value_sub('filter', 'base', results[1])
                if results[2] is not None:
                    self.marc.add_value_sub('filter', 'second', results[2])
                self.marc.add_value_sub('filter', 'number', results[3])

    def create_call_number_filter(self) -> Optional[Tuple[str, Optional[str], Optional[str], str]]:
        call_number = ''
        if len(self.marc.result['call_number']) == 1:
            call_number = self.marc.result['call_number'][0]
        else:
            for call_n in self.marc.result['call_number']:
                if call_n.startswith('UBH'):
                    call_number = call_n

        call_number = re.sub('\s+', ' ', call_number.strip())

        database = self.marc.result['database']

        if database == 'dsv05' and call_number != '':
            call_number = 'HAN ' + call_number

        if call_number == '':
            # remove call number if it is empty.
            del self.marc.result['call_number']
            return None

        if not re.match('(UBH|HAN)', call_number) or re.fullmatch('UBH', call_number):
            # ignore anything which does not comply with convention.
            return None

        simple = re.fullmatch('(\w+) ([\w\-*.]+) (\d+)(.*)?', call_number)
        if simple:
            return simple.group(1), simple.group(2), None, (simple.group(3) + simple.group(4)).strip()

        word_roman = re.fullmatch('(\w+) (\w+) ([MCLXVI]+[ ]?[a-z]?) (\d+)(.*)?', call_number)
        if word_roman:
            return word_roman.group(1), \
                   word_roman.group(2), \
                   word_roman.group(3),  \
                   (word_roman.group(4) + word_roman.group(5)).strip()

        double_word_roman = re.fullmatch('(\w+) ([\w\-*]+) ([\w\-*]+) ([MCLXVI]+[ ]?[a-z]?) (\d+)(.*)?',
                                         call_number)
        if double_word_roman:
            return double_word_roman.group(1), \
                   double_word_roman.group(2) + ' ' + double_word_roman.group(3), \
                   double_word_roman.group(4), \
                   double_word_roman.group(5)

        three_word = re.fullmatch('(\w+) ([\w\-*]+) ([\w\-*]+) ([A-Za-z\-*]+)(.*)?', call_number)
        if three_word:
            return three_word.group(1), \
                   three_word.group(2) + ' ' + three_word.group(3), \
                   three_word.group(4), \
                   three_word.group(5).strip()

        double_word = re.fullmatch('(\w+) ([\w\-*]+) ([\w\-*]+)(.*)?', call_number)
        if double_word:
            return double_word.group(1), double_word.group(2), double_word.group(3), double_word.group(4).strip()

        rest_han = re.fullmatch('(HAN) (.*)', call_number)
        if rest_han:
            return rest_han.group(1), None, None, rest_han.group(2).strip()

        rest_ubh = re.fullmatch('(UBH) (.*)', call_number)
        if rest_ubh:
            return rest_ubh.group(1), None, None, rest_ubh.group(2).strip()

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





