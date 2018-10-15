from simple_elastic import ElasticIndex
import json
import re
import requests


e_codices_doi_prefix = '10.5076/e-codices-ubb-{}'
ubb_page_stem = re.compile(r'/ubb/(([A-Za-z]{1,2}-[IVXivx]+-)?([0][0-9]{3}.*?))(\?|&|/|$)')


def read_file(file_name: str):
    with open(file_name, 'r') as fp:
        return json.load(fp)


def transform_call_number(call_number: str) -> str:
    result = ''
    parts = re.match('(([A-Z]{1,2}-[IVX]+-)?([0-9]{4})(([a-z])|(.*)))', call_number)
    if parts.group(2):
        result += parts.group(2).replace('-', ' ')

    if parts.group(3):
        result += parts.group(3).replace('0', '')

    if parts.group(4):
        if re.fullmatch('[a-z]', parts.group(4)):
            result += parts.group(4)
        else:
            result += ':' + parts.group(4).strip('-')
    if result == 'BC II 5':
        result = 'Bc II 5'
    elif result == 'LE VI 12':
        result = 'Le VI 12 Einband'
    elif result == 'N I 3:13-15':
        result = 'N I 3:13 + 15'
    elif result == 'N I 6:67':
        result = result + 'a+b'
    elif result == 'N I 6:71':
        result = result + 'a+b'
    elif result == 'N I 1:3ab':
        result = 'N I 1:3a+b'
    return result


if __name__ == '__main__':
    data = dict()
    for year in range(2012, 2019):
        data[str(year)] = read_file('data/output-{}-01-01-{}-12-31.json'.format(year, year))

    call_numbers_encoded = set()
    collect_stems = dict()
    for year in data:
        for page_stem in data[year]:
            match = ubb_page_stem.search(page_stem)
            if match:
                if match.group(2):
                    call_number = match.group(2).upper() + match.group(3)
                else:
                    call_number = match.group(3)

                if call_number not in collect_stems:
                    collect_stems[call_number] = dict()

                if year not in collect_stems[call_number]:
                    collect_stems[call_number][year] = data[year][page_stem]
                else:
                    collect_stems[call_number][year] += data[year][page_stem]
                call_numbers_encoded.add(call_number)

    for key in collect_stems:
        total = 0
        for year in range(2012, 2019):
            if str(year) in collect_stems[key]:
                total += collect_stems[key][str(year)]
            else:
                collect_stems[key][str(year)] = 0
        collect_stems[key]['total'] = total

    dois = dict()

    accepted_dois = list()
    rejected_dois = list()
    for item in call_numbers_encoded:
        doi = e_codices_doi_prefix.format(item)
        response = requests.get('https://doi.org/{}'.format(doi))
        if response.ok:
            accepted_dois.append(doi)
            dois[item] = doi
        else:
            rejected_dois.append(doi)

    with open('suppl/accepted-dois.json', 'w') as fp:
        json.dump(accepted_dois, fp, indent=4, ensure_ascii=False)

    with open('suppl/rejected-dois.json', 'w') as fp:
        json.dump(rejected_dois, fp, indent=4, ensure_ascii=False)

    with open('suppl/call-numbers.json', 'w') as fp:
        json.dump(sorted(list(call_numbers_encoded)), fp, indent=4, ensure_ascii=False)

    with open('suppl/output.json', 'w') as fp:
        json.dump(collect_stems, fp, indent=2, ensure_ascii=False)
    target = ElasticIndex('e-codices-data', 'hits')
    index = ElasticIndex('kafka-dsv05-*', 'record')
    for key in collect_stems:
        if key == '0001':
            # TODO: A combined manuscript.
            continue

        item = dict()

        cn = transform_call_number(key)
        query = {
            '_source': ['call_number', 'identifiers.*'],
            'query': {
                'term': {
                    'call_number.keyword': cn
                }
            }
        }

        results = index.scan_index(query=query)

        if len(results) == 1:
            result = results[0]
            item['hits'] = collect_stems[key]
            if 'doi' not in result['identifiers']:
                if key in dois:
                    item['doi'] = dois[key]

            target.index_into(item, result['identifiers']['dsv05'])
        else:
            print(key, cn, results)






