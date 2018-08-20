
from simple_elastic import ElasticIndex
from collections import Counter

from roman import fromRoman, InvalidRomanNumeralError
import json
import re


find_roman_numeral = re.compile('([MCLXVI]+)[^a-z]')
roman_numeral = re.compile('^M{0,4}(CM|CD|D?C{0,3})(XC|XL|L?X{0,3})(IX|IV|V?I{0,3})$')

empty = re.compile('\s+v\.')


pages_regex = re.compile('[\[]?(\d+)[\]]? (S|Seiten|p|Bl(ätter)?)')
vol = re.compile('([A-Za-zäöü]+[.]?) ([0-9]+)')
ser = re.compile('([A-Za-zäöü]+[.]?) ([0-9]+)-([0-9]+)')


if __name__ == '__main__':
    index = ElasticIndex('kafka*', 'record')

    c = Counter()
    total = Counter()

    total_pages_music = 0

    query = {
        '_source': ['extent.coverage', 'c-format'],
        'query': {
            'exists': {
                'field': 'extent.coverage'
            }
        }
    }

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
                match = pages_regex.search(coverage)
                if match:
                    print(coverage)
                    pages += int(match.group(1))
                    total_pages_music += pages

                match = find_roman_numeral.search(coverage)
                if match:
                    roman_number = roman_numeral.fullmatch(match.group(1))
                    if roman_number:
                        pages += fromRoman(roman_number.group(0))
                if pages > 0:
                    c['music_pages'] += 1
                    # return X, Seiten
                    continue
                else:
                    band = 0
                    match = re.search('(\d+) (Abt|B|C|H[^y]|He|K|[Pp]art|Ser|T|[Vv]ol)', coverage)

                    if match:
                        band += int(match.group(1))
                        c['music_vol'] += 1
                        continue
                    else:
                        c['music_else'] += 1
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

    print('Partitur (Average pages): {}'.format(total_pages_music / c['music_pages']))
