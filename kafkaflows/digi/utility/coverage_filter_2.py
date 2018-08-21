
from simple_elastic import ElasticIndex
from roman import fromRoman

from collections import Counter
from statistics import median
import re

regex_find_all_words = re.compile('([A-Z]\w+[\w ]+)')

regex_find_all_number_word = re.compile('(\d+) (\w+)')
regex_find_all_number_word_series = re.compile('(\d+) (\w+[\w .]+)')
regex_find_all_word_number = re.compile('(\w+)[.]? (\d+)([^-]|$)')
regex_find_all_word_number_series = re.compile('(\w+)[.]? (\d+)-(\d+)')

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

regex_pages_single = re.compile('(S|Bl)[.]? (\d+)')
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


def parse_archive(coverage, return_type):
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
        return 0, 'None'


def parse_meters(coverage):
    lfm = 0
    results = regex_laufmeter.findall(coverage)
    for result in results:
        lfm += float(result[0].replace(',', '.'))

    if lfm > 0:
        return lfm, 'Laufmeter'
    else:
        return 0, 'None'


def parse_folders(coverage, return_type):
    folders = 0
    results = regex_folders.findall(coverage)
    for result in results:
        folders += int(result[0])

    if folders > 0:
        return folders, return_type
    else:
        return 0, 'None'


def parse_boxes(coverage):
    boxes = lfm = 0
    results = regex_boxes.findall(coverage)
    for result in results:
        if result[3] == '':
            boxes += int(result[0])
        else:
            lfm += float(result[4].replace(',', '.'))

    if boxes > 0:
        return boxes, 'Schachteln'
    elif lfm > 0:
        return lfm, 'Laufmeter'
    else:
        return 0, 'None'


def parse_letters(coverage):
    letters = 0
    results = regex_letters.findall(coverage)
    for result in results:
        letters += int(result[0])

    if letters > 0:
        return letters, 'Briefe'
    else:
        return 0, 'None'


def parse_volumes(coverage, return_type):
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
        return 0, 'None'


def parse_pages(coverage):
    pages = 0
    results = regex_pages.findall(coverage)
    for result in results:
        pages += int(result[0])

    results = regex_pages_single.findall(coverage)
    for result in results:
        pages += int(result[1])

    results = regex_pages_range.findall(coverage)
    for result in results:
        pages += int(result[2]) - int(result[1]) + 1

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
        return pages, 'Seiten'
    else:
        return 0, 'None'


if __name__ == '__main__':
    index = ElasticIndex('kafka*', 'record')

    c = Counter()
    total = Counter()

    volumes_counter = Counter()
    series_counter = Counter()
    words_counter = Counter()
    words_only_counter = Counter()
    words_series_counter = Counter()

    total_pages_music = list()
    total_pages_maps = list()
    total_pages_atlas = list()
    total_pages_letter = list()
    total_pages_article = list()
    total_pages_law = list()
    total_pages_manuscript = list()
    total_pages_foto = list()

    query = {
        '_source': ['extent.coverage', 'c-format'],
        'query': {
            'exists': {
                'field': 'extent.coverage'
            }
        }
    }

    all_volumes = set()

    for results in index.scroll(query=query):
        for record in results:
            total['total'] += 1
            coverage = record['extent']['coverage']

            if record['c-format'] in ['Objekt', 'Diverse Tonformate', 'Schallplatte', 'Diverse Filmformate', 'Datenbank']:
                c['other'] += 1
                continue
            elif record['c-format'] in ['Klavierauszug', 'Partitur', 'Noten']:
                if empty.search(coverage):
                    c['music-empty'] += 1
                    # return 1, 'Partitur'
                    continue

                num, name = parse_pages(coverage)
                if name == 'Seiten':
                    total_pages_music.append(num)
                    # return num, name
                    c['music-pages'] += 1
                    continue

                num, name = parse_volumes(coverage, 'Partitur')
                results = re.findall('(\d+) (Abt|B|C|H$|He|K|[Pp]art|Ser|St|T|[Vv]ol)', coverage)
                for result in results:
                    num += int(result[0])

                if num > 0:
                    # return num, name
                    c['music-volumes'] += 1
                    continue

                num, name = parse_meters(coverage)
                if num > 0:
                    # return num, name
                    c['music-laufmeter'] += 1
                    continue

                # return 1, 'Partitur'
                c['music-default'] += 1
                continue
            elif record['c-format'] in ['Atlas', 'Karte', 'Diverse Kartenformate']:

                # Median Map pages = 4
                # Median Maps =
                if empty.fullmatch(coverage):
                    # return 1, 'Karten'
                    c['maps-none'] += 1
                    continue

                num, name = parse_pages(coverage)
                if name == 'Seiten':
                    if record['c-format'] in ['Atlas']:
                        total_pages_atlas.append(pages)
                        c['atlas-pages'] += 1
                        continue
                    else:
                        total_pages_maps.append(pages)
                        c['maps-pages'] += 1
                        continue

                maps_matches = re.findall('(\d+) ([Kc]arte[n]?|Pl[äa]n[e]?|Vogel|Ansicht|Panorama|Manuskript)', coverage)
                maps = 0
                for matches in maps_matches:
                    maps += int(matches[0])

                if maps > 0:
                    # return maps, 'Karten'
                    c['maps-maps'] += 1
                    continue
                atlas_matches = re.findall('(\d+) (Atlas)', coverage)

                atlas = 0
                for match in atlas_matches:
                    atlas += int(match[0])
                if atlas > 0:
                    # return atlas, 'Atlas'
                    c['atlas'] += 1
                    continue
                folders, name = parse_folders(coverage, 'Kartenmappe')
                if folders > 0:
                    # return atlas, 'Kartenmappe'
                    c['maps-folders'] += 1
                    continue

                c['maps-default'] += 1
                # Average of all
                # return 1, 'Karte'
                continue
            elif record['c-format'] in ['Zeitung', 'Zeitschrift / Schriftenreihe']:
                c['series'] += 1
                continue
            elif record['c-format'] in ['Brief', 'Briefsammlung']:
                if empty.fullmatch(coverage):
                    # return 1, 'Briefe'
                    c['letter-none'] += 1
                    continue

                pages, name = parse_pages(coverage)

                letter_single_page_types = re.findall('(\d+) (Karte|Briefkarte|Postkarte|Ansichtskarte|Visitenkarte)', coverage)
                for result in letter_single_page_types:
                    pages += int(result[0])

                letter_single_page_words = re.match('Briefkarte|Postkarte|Zettel|Karte|Visitenkarte', coverage)
                if letter_single_page_words:
                    pages += 1

                if pages > 0:
                    # return pages, 'Seiten'
                    total_pages_letter.append(pages)
                    c['letter-pages'] += 1
                    continue

                letters, name = parse_letters(coverage)

                if letters > 0:
                    # return letters, 'Briefe'
                    c['letter-letters'] += 1
                    continue

                volumes, name = parse_volumes(coverage, 'Briefband')

                if volumes > 0:
                    # return volumes, 'Briefband'
                    c['letter-volumes'] += 1
                    continue

                folders = 0
                results = regex_folders.findall(coverage)
                for result in results:
                    folders += int(result[0])

                if folders > 0:
                    # return folders, 'Briefmappe'
                    c['letter-folders'] += 1
                    continue

                # return 2, 'Briefe'
                c['letter-default'] += 1
                continue
            elif record['c-format'] in ['Diverse Bildformate', 'Fotografie']:
                if empty.fullmatch(coverage):
                    # return 1, 'Seiten'
                    c['fotos-none'] += 1
                    continue

                pages, name = parse_pages(coverage)

                results = re.findall('(\d+) (Kupferstich|Litho|Foto|Zeichnung|Repro|Holzschnitt|Schattenriss'
                                     '|Aquarell|Druckgrafik(en)?|Physionotrace|Bild|Stück|Radierung)', coverage)
                for result in results:
                    pages += int(result[0])

                if pages > 0:
                    total_pages_foto.append(pages)
                    c['foto-pages'] += 1
                    continue

                folders = 0
                results = re.findall('(\d+) (Skizzenbuch|Mappe[n]?)', coverage)
                for result in results:
                    folders += int(result[0])

                if folders > 0:
                    # return folders, 'Fotomappe'
                    c['fotos-folders'] += 1
                    continue

                # return 1, 'Seiten'
                c['fotos-default'] += 1
                continue
            elif record['c-format'] in ['Gesamtwerk', 'Buch', 'Verfassung / Gesetz']:
                if empty.fullmatch(coverage):
                    # return 1, 'Band'
                    c['book-none'] += 1
                    continue

                num, name = parse_pages(coverage)
                if name == 'Seiten':
                    total_pages_law.append(num)
                    c['book-pages'] += 1
                    continue

                volumes, name = parse_volumes(coverage, 'Band')

                if volumes > 0:
                    # return volumes, 'Band'
                    c['book-volumes'] += 1
                    continue

                # return 1, 'Band'
                c['book-default'] += 1
                continue
            elif record['c-format'] in ['Artikel']:
                if empty.fullmatch(coverage):
                    # return 1, 'Artikel'
                    c['articles-none'] += 1
                    continue

                num, name = parse_pages(coverage)
                if name == 'Seiten':
                    total_pages_article.append(num)
                    c['articles-pages'] += 1
                    continue

                volumes, name = parse_volumes(coverage, 'Band')
                if volumes > 0:
                    # return volumes, 'Band'
                    c['articles-volumes'] += 1
                    continue

                # return 1, 'Artikel'
                c['articles-default'] += 1
                continue
            elif record['c-format'] in ['Handschrift']:
                if empty.fullmatch(coverage):
                    # return 1, 'Handschrift'
                    c['manuscript-none'] += 1
                    continue

                num, name = parse_pages(coverage)
                if name == 'Seiten':
                    total_pages_manuscript.append(num)
                    c['manuscript-pages'] += 1
                    continue

                volumes, name = parse_volumes(coverage, 'Manuskriptband')

                results = re.findall('(\d+) (Stücke|Papiertüte[n]?|Faszikel|Dossier|Broschüre|Zeichenbuch|'
                                     'Heft(e|chen)?|Schuber|Bündel|Konvolut|Schulheft|Umschläge|Büchlein|Umschlag|Predigten)', coverage)
                for result in results:
                    volumes += int(result[0])

                if volumes > 0:
                    # return volumes, 'Band'
                    c['manuscript-volumes'] += 1
                    continue

                folders = 0
                results = regex_folders.findall(coverage)
                for result in results:
                    folders += int(result[0])

                if folders > 0:
                    # return folders, 'Manuskriptmappen'
                    c['manuscript-folders'] += 1
                    continue

                num, name = parse_boxes(coverage)

                if name == 'Schachteln':
                    # return boxes, 'Manuskriptschachteln'
                    c['manuscript-boxes'] += 1
                    continue
                elif name == 'Laufmeter':
                    # return boxes, 'Manuskriptschachteln'
                    c['manuscript-lfm'] += 1
                    continue

                letters, name = parse_letters(coverage)

                if letters > 0:
                    # return letters, 'Briefe'
                    c['manuscript-letters'] += 1
                    continue

                # return 1, 'Manuskriptband'
                c['manuscript-default'] += 1
                continue
            elif record['c-format'] in ['Dossier']:
                if empty.fullmatch(coverage):
                    # return 1, 'Dossier'
                    c['dossier-none'] += 1
                    continue

                pages, name = parse_pages(coverage)
                if pages > 0:
                    c['dossier-pages'] += 1
                    continue

                volumes, name = parse_volumes(coverage, 'Band')
                if volumes > 0:
                    c['dossier-volumes'] += 1
                    continue

                boxes, name = parse_boxes(coverage)
                if boxes > 0:
                    c['dossier-boxes'] += 1
                    continue

                folders, name = parse_folders(coverage, 'Mappen')
                if folders > 0:
                    c['dossier-folders'] += 1
                    continue

                lfm, name = parse_meters(coverage)
                if lfm > 0:
                    c['dossier-lfm'] += 1
                    continue

                letters, name = parse_letters(coverage)
                if letters > 0:
                    c['dossier-letters'] += 1
                    continue

                archives, name = parse_archive(coverage, 'Archiveinheit')
                if archives > 0:
                    c['dossier-archive'] += 1
                    continue

                # return 2, 'Schachteln'
                c['dossier-default'] += 1
                continue

    for x in sorted(c.items()):
        print(x)
    print('Total: {}'.format(sum(c.values())))
    print('Total records: {}'.format(sum(total.values())))

    print('Maps (Average pages): {}'.format(sum(total_pages_maps) / c['maps-pages']))
    print('Maps (Median pages): {}'.format(median(total_pages_maps)))
    print('Atlas (Average pages): {}'.format(sum(total_pages_atlas) / c['atlas-pages']))
    print('Atlas (Median pages): {}'.format(median(total_pages_atlas)))
    print('Partitur (Average pages): {}'.format(sum(total_pages_music) / c['music-pages']))
    print('Partitur (Median pages): {}'.format(median(total_pages_music)))
    print('Letters (Average pages): {}'.format(sum(total_pages_letter) / c['letter-pages']))
    print('Letters (Median pages): {}'.format(median(total_pages_letter)))
    print('Articles (Average pages): {}'.format(sum(total_pages_article) / c['articles-pages']))
    print('Articles (Median pages): {}'.format(median(total_pages_article)))
    print('Law (Average pages): {}'.format(sum(total_pages_law) / c['law-pages']))
    print('Law (Median pages): {}'.format(median(total_pages_law)))
    print('Manuscript (Average pages): {}'.format(sum(total_pages_manuscript) / c['manuscript-pages']))
    print('Manuscript (Median pages): {}'.format(median(total_pages_manuscript)))

if False:
    print(coverage)
    result = regex_find_all_number_word.findall(coverage)
    for matches in result:
        words_counter.update([matches[1]])

    result = regex_find_all_number_word_series.findall(coverage)
    for matches in result:
        words_series_counter.update([matches[1]])

    result = regex_find_all_word_number.findall(coverage)
    for matches in result:
        volumes_counter.update([matches[0]])

    result = regex_find_all_word_number_series.findall(coverage)
    for matches in result:
        series_counter.update([matches[0]])

    result = regex_find_all_words.fullmatch(coverage)
    if result:
        words_only_counter.update([result.group(0)])