from simple_elastic import ElasticIndex

from collections import Counter
import re

if __name__ == '__main__':
    index = ElasticIndex('kafka*', 'record')

    query = {
        '_source': ['call_number', 'database', 'final.format'],
        'query': {
            'exists': {
                'field': 'call_number'
            }
        }
    }

    counter = Counter()
    prefix_counter = Counter()
    matched_counter = Counter()

    for results in index.scroll(query=query):
        for record in results:
            counter['total'] += 1

            call_number = ''
            if len(record['call_number']) == 1:
                call_number = record['call_number'][0]
            else:
                for call_n in record['call_number']:
                    if call_n.startswith('UBH'):
                        call_number = call_n

            call_number = re.sub('\s+', ' ', call_number.strip())

            database = record['database']

            if database == 'dsv05' and call_number != '':
                call_number = 'HAN ' + call_number

            if call_number == '' or not re.match('(UBH|HAN)', call_number) or re.fullmatch('UBH', call_number):
                # discard anything which does not comply with convention.
                matched_counter['discarded'] += 1
                continue

            prefix_match = re.match('(\w+)', call_number)
            prefix_counter.update([prefix_match.group(1)])

            simple = re.fullmatch('(\w+) ([\w\-*.]+) (\d+)(.*)?', call_number)
            if simple:
                prefix = simple.group(1)
                base = simple.group(2)
                number = simple.group(3)
                matched_counter['word'] += 1
                continue

            word_roman = re.fullmatch('(\w+) (\w+) ([MCLXVI]+[ ]?[a-z]?) (\d+)(.*)?', call_number)
            if word_roman:
                prefix = word_roman.group(1)
                base = word_roman.group(2)
                second = word_roman.group(3)
                number = word_roman.group(4)
                matched_counter['word_roman'] += 1
                continue

            double_word_roman = re.fullmatch('(\w+) ([\w\-*]+) ([\w\-*]+) ([MCLXVI]+[ ]?[a-z]?) (\d+)(.*)?', call_number)
            if double_word_roman:
                prefix = double_word_roman.group(1)
                base = double_word_roman.group(2) + ' ' + double_word_roman.group(3)
                second = double_word_roman.group(4)
                number = double_word_roman.group(5)
                matched_counter['double_word_roman'] += 1
                continue

            three_word = re.fullmatch('(\w+) ([\w\-*]+) ([\w\-*]+) ([\w\-*]+)(.*)?', call_number)
            if three_word:
                prefix = three_word.group(1)
                base = three_word.group(2) + ' ' + three_word.group(3)
                second = three_word.group(4)
                number = three_word.group(5).strip()
                matched_counter['three_word'] += 1
                continue

            double_word = re.fullmatch('(\w+) ([\w\-*]+) ([\w\-*]+)(.*)?', call_number)
            if double_word:
                prefix = double_word.group(1)
                base = double_word.group(2)
                second = double_word.group(3)
                number = double_word.group(4).strip()
                matched_counter['double_word'] += 1
                continue

            rest_han = re.fullmatch('(HAN) (.*)', call_number)
            if rest_han:
                prefix = rest_han.group(1)
                number = rest_han.group(2)
                matched_counter['rest-han'] += 1
                continue

            rest_ubh = re.fullmatch('(UBH) (.*)', call_number)
            if rest_ubh:
                prefix = rest_ubh.group(1)
                number = rest_ubh.group(2)
                matched_counter['rest-ubh'] += 1
                continue

    for c in matched_counter.items():
        print(c)

    print('Total matched: ' + str(sum(matched_counter.values())))

    for c in prefix_counter.items():
        print(c)

    print('Total prefix: ' + str(sum(prefix_counter.values())))

    for c in counter.items():
        print(c)

