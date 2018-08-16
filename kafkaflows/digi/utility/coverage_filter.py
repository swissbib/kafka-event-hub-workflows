from simple_elastic import ElasticIndex
from collections import Counter

from roman import fromRoman, InvalidRomanNumeralError
import json
import re

if __name__ == '__main__':
    index = ElasticIndex('kafka*', 'record')

    c = Counter()
    alt_c = Counter()

    query = {
        '_source': ['extent.coverage', 'c-format'],
        'query': {
            'exists': {
                'field': 'extent.coverage'
            }
        }
    }

    missing = open('missing.txt', 'w')
    find_all_result = open('find_all.txt', 'w')

    find_all = list()
    descriptive_coverage = list()
    desc_cov_add = list()
    vols_cov = list()
    words = list()

    find_all_w = set()
    find_all_c = set()
    count = 0
    for results in index.scroll(query=query):
        for record in results:
            if record['c-format'] in ['DVD / CD', 'Datenbank', 'Objekt', 'Diverse Tonformate']:
                c['other'] += 1
                continue

            if record['c-format'] in ['Atlas']:
                c['atlas'] += 1
                continue

            count += 1
            coverage = record['extent']['coverage']

            match_desc = re.fullmatch('(?P<number>[0-9]+) (?P<word>[A-Za-zöäü \-]+(\.)?)', coverage)
            match_2 = re.match('(?P<number>[0-9]+) (?P<word>[A-Za-zöäü \-]+(\.)?) \(.*\)$', coverage)
            match_3 = re.match('(?P<word>[A-Za-zäöü.]+) (?P<number>[0-9]+([,./\-][0-9]+)?)$', coverage)

            # Words only. Ignore.
            match_word_only = re.fullmatch('(?P<words>[A-Za-zäöü.\-()]+)', coverage)

            # X Bl. ; A4/A5/X cm
            match_postcard = re.fullmatch('(?P<number>[0-9]+) Bl\. ; '
                                          '((?P<format>[ ]?[456])|(?P<size>[0-9]+)[ ]?cm)', coverage)

            # Laufmeter
            match_lfm = re.fullmatch('([Cc]a\. )?(?P<number>[0-9]+,[0-9]+) (m|Lfm|Laufmeter|lfd.m)( \(.*\))?', coverage)

            # Roman numbers with page number.
            match_pages_roman = re.fullmatch(
                '(?P<roman>[CVXILMcvixlm]+)[,.] (?P<number>[0-9]+) ([Ss](eiten|.)?|p(ages)?)$', coverage)

            # Find letters.
            letter = re.search('Brief[e]?', coverage)
            photo = re.search('(Ph|F)oto', coverage)

            half_pages = re.fullmatch('([0-9]+)([½¾]|[.,][0-9]+| [0-9]/[0-9]) (Bl|S)\.', coverage)

            find_all_with_addition = re.findall('([0-9]+) (\w+)[ ]?(\(([0-9]+) (\w+)\))?', coverage)


            for i in find_all_with_addition:
                find_all_w.add(i[1])
                find_all_c.add(i[4])

            if re.match('\s+v\.$', coverage):
                # DONE
                c['none'] += 1
                continue
            if re.match('(\[)?[0-9]+(\])? ([Ss](eiten|.)?|p(ages)?)$', coverage):
                # DONE
                c['pages'] += 1
                continue

            if match_pages_roman:
                # DONE
                result = match_pages_roman.groupdict()
                try:
                    roman = fromRoman(result['roman'])
                except InvalidRomanNumeralError:
                    pages = int(result['number'])
                else:
                    pages = roman + int(result['number'])

                if pages > 0:
                    c['roman_pages_p_normal_pages'] += 1
                    continue
            if match_lfm:
                # DONE
                lfm = match_lfm.groupdict()
                meters = float(lfm['number'].replace(',', '.'))

                if meters > 0:
                    c['lfm'] += 1
                    continue
            if match_postcard:
                # DONE
                pages = int(match_postcard.groupdict()['number'])

                if pages > 0:
                    c['postcards'] += 1
                    continue
            if half_pages:
                # DONE
                pages = int(half_pages.group(1)) + 1

                if pages > 0:
                    c['half_pages'] += 1
                    continue

            if letter:
                # DONE
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
                            pages += int(l[3])
                        elif l[4] in ['Blatt', 'Bl']:
                            pages += int(l[3])

                    if pages > 0:
                        c['letter'] += 1
                        continue

            if photo:
                c['photo'] += 1
                continue

            if len(find_all_with_addition) > 0:
                pages = 0
                volumes = 0
                for l in find_all_with_addition:
                    if l[1] in ['Bl', 'Blatt', 'Karte', 'Karten', 'Dokumente', 'Fotografie', 'Fotografien', 'S',
                                'Bogen', 'Girozettel']:
                        pages += int(l[0])
                    elif l[1] in ['Band'] and l[4] in ['Blätter']:
                        pages += int(l[3])
                    elif l[1] in ['Band', 'Bände', 'Mappe', 'Mappen', 'Hefte', 'Heft', 'Schachtel',
                                  'Schachteln', 'Konvolute'] \
                            and l[4] == '':
                        volumes += int(l[0])
                if pages > 0 or volumes > 0:
                    c['find_all'] += 1
                    continue

            if match_desc:
                descriptive_coverage.append(match_desc.groupdict())
                c['desc'] += 1
            elif match_2:
                desc_cov_add.append(match_2.groupdict())
                c['desc_klammer'] += 1
            elif match_3:
                vols_cov.append(match_3.groupdict())
                c['vols'] += 1
            elif match_word_only:
                words.append(match_word_only.groupdict())
                c['words'] += 1
            else:
                matches = re.findall('[\[]?(?P<number>[0-9]+)[\]]? '
                                     '(mit |und )?'
                                     '(?P<word>[A-Za-zöäü ]+[A-Za-zöäü]+)'
                                     '(.,;| mit| und)?', coverage)

                c['else'] += 1
                if len(matches) > 0:
                    find_all.extend([f_match[2] for f_match in matches])
                    find_all_result.write(str(matches) + '\n')
                else:
                    missing.write(coverage + '\n')

    singular = set()
    plural = set()
    for d in descriptive_coverage:
        if d['number'] == '1':
            singular.add(d['word'])
        else:
            plural.add(d['word'])

    with open('singular.json', 'w') as f:
        json.dump(sorted(list(singular)), f, ensure_ascii=False, indent='    ')

    with open('plural.json', 'w') as f:
        json.dump(sorted(list(plural)), f, ensure_ascii=False, indent='    ')

    vols = set()
    for v in vols_cov:
        vols.add(v['word'])

    with open('vols.json', 'w') as f:
        json.dump(sorted(list(vols)), f, ensure_ascii=False, indent='    ')

    set_words = set()
    for w in words:
        set_words.add(w['words'])
    with open('words.json', 'w') as f:
        json.dump(sorted(list(set_words)), f, ensure_ascii=False, indent='    ')

    all = set(find_all)
    with open('all.json', 'w') as f:
        json.dump(sorted(list(all)), f, ensure_ascii=False, indent='    ')

    for x in c.items():
        print(x)
    print('Total: {}'.format(sum(c.values())))
    print('Total records: {}'.format(count))

    with open('find_all_addition.json', 'w') as f:
        json.dump(sorted(list(find_all_w)), f, ensure_ascii=False, indent='    ')

    with open('find_all_klammer.json', 'w') as f:
        json.dump(sorted(list(find_all_c)), f, ensure_ascii=False, indent='    ')

    missing.close()
    find_all_result.close()
