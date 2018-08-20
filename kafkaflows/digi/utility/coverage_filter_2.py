
from simple_elastic import ElasticIndex
from roman import fromRoman, InvalidRomanNumeralError

from collections import Counter
from statistics import median
import json
import re


find_roman_numeral = re.compile('([MCLXVI]+)[^a-z]')
roman_numeral = re.compile('^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')

empty = re.compile('\s+v\.')


regex_pages = re.compile('[\[]?(\d+)[\]]? (S( |\.|,|\)|$)|Seiten|p|Bl(ätter)?|Bogen|P$)')
regex_pages_part = re.compile('S\. (\d+)-(\d+)')
regex_vol = re.compile('(No|Vol|Bd|Nr|H|Heft)[.]?[ ]?([0-9]+)(\.([0-9]+))?([ \.,]|$)')
regex_series = re.compile('(No|H|Nr|Heft)[.]?[ ]?([0-9]+)-([0-9]+)')
regex_series_open = re.compile('(No|H|Nr|Heft)[.]?[ ]?([0-9]+)-')
regex_laufmeter = re.compile('(\d+,\d+) Lfm')


if __name__ == '__main__':
    index = ElasticIndex('kafka*', 'record')

    c = Counter()
    total = Counter()
    partitur_volumes = Counter()
    partitur_series = Counter()

    total_pages_music = list()
    series_number = list()

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
                    c['music_empty'] += 1
                    # return 1, Partitur
                    continue
                pages = 0
                match = regex_pages.search(coverage)
                if match:
                    pages += int(match.group(1))

                match = find_roman_numeral.search(coverage)
                if match:
                    roman_number = roman_numeral.fullmatch(match.group(1))
                    if roman_number:
                        pages += fromRoman(roman_number.group(0))

                match = regex_pages_part.search(coverage)
                if match:
                    # Bsp: S. 24-35
                    pages = int(match.group(2)) - int(match.group(1)) + 1

                if pages > 0:
                    c['music_pages'] += 1
                    total_pages_music.append(pages)
                    # return X, Seiten
                    continue
                else:
                    band = 0
                    match = re.search('(\d+) (Abt|B|C|H$|He|K|[Pp]art|Ser|St|T|[Vv]ol)', coverage)
                    volume = regex_vol.findall(coverage)
                    series = regex_series.findall(coverage)
                    lfm = regex_laufmeter.fullmatch(coverage)
                    if match:
                        band += int(match.group(1))
                        c['music-vol'] += 1
                        continue
                    elif len(volume) > 0:
                        # return 1, 'Partitur'
                        c.update(['music-vol'])
                        continue
                    elif lfm:
                        lfm = float(lfm.group(1).replace(',', '.')) * 8000
                        # return lfm, 'Seiten'
                        c['music-lfm'] += 1
                        continue
                    elif len(series) > 0:
                        # return series[0][2] - series[0][1] + 1, 'Partitur'
                        c.update(['music-partitur'])
                        continue
                    else:
                        # return 3, 'Partitur'
                        print(coverage)
                        c['music-partitur'] += 1
                        continue
            elif record['c-format'] in ['Atlas', 'Karte', 'Diverse Kartenformate']:
                c['maps'] += 1
                continue
            elif record['c-format'] in ['Zeitung', 'Zeitschrift/Schriftenreihe']:
                c['series'] += 1
            elif record['c-format'] in ['Brief', 'Briefsammlung']:
                c['letters'] += 1
            elif record['c-format'] in ['Diverse Bildformate', 'Fotografie']:
                c['images'] += 1
            elif record['c-format'] in ['Verfassung / Gesetz']:
                c['law'] += 1
            elif record['c-format'] in ['Gesamtwerk', 'Buch']:
                c['books'] += 1
            elif record['c-format'] in ['Artikel']:
                c['articles'] += 1
            elif record['c-format'] in ['Handschrift']:
                c['manuscript'] += 1
            elif record['c-format'] in ['Dossier']:
                c['dossier'] += 1

    for x in c.items():
        print(x)
    print('Total: {}'.format(sum(c.values())))
    print('Total records: {}'.format(sum(total.values())))

    print('Partitur (Average pages): {}'.format(sum(total_pages_music) / c['music_pages']))
    print('Partitur (Median pages): {}'.format(median(total_pages_music)))

    print('Volumes')
    for x in partitur_volumes.items():
        if x[1] > 10:
            print(x)

    print('Series')
    for x in partitur_series.items():
        if x[1] > 10:
            print(x)

    print(median(series_number))