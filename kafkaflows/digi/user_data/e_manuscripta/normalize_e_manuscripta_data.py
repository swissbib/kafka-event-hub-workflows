from simple_elastic import ElasticIndex

import os
import re
import json


def get_vlids(path: str):
    vlids = dict()

    all_vlids_per_plattform = dict()

    for root, dirs, files in os.walk(path):
        for file in files:
            place, collection, year = file.split('.')[0].split('-')
            p = place + '-' + collection
            if p not in vlids:
                vlids[p] = dict()
            if year not in vlids[p]:
                vlids[p][year] = dict()
            if p not in all_vlids_per_plattform:
                all_vlids_per_plattform[p] = set()
            with open(root + file, 'r') as fp:
                record = json.load(fp)
                for item in record['data']:
                    stem = item['dimensions']['pagestem'].strip()
                    base = stem.split('?')[0]
                    vlid = base.split('/')[-1]
                    try:
                        _ = int(vlid)
                    except ValueError:
                        match = re.search('doi/(.*)', stem)
                        if match:
                            doi = match.group(0)
                            if doi not in vlids[p][year]:
                                vlids[p][year][doi] = dict()
                                vlids[p][year][doi]['page-views'] = int(item['metrics']['pageviews'])
                            else:
                                vlids[p][year][doi]['page-views'] += int(item['metrics']['pageviews'])
                    else:
                        if vlid not in vlids[p][year]:
                            vlids[p][year][vlid] = dict()
                            vlids[p][year][vlid]['page-views'] = int(item['metrics']['pageviews'])
                        else:
                            vlids[p][year][vlid]['page-views'] += int(item['metrics']['pageviews'])
                        all_vlids_per_plattform[p].add(int(vlid))

    return vlids, all_vlids_per_plattform


def get_mapping_vlids_sys_num(path: str):
    full_mapping = dict()
    reverse_mapping = dict()
    all_per_plattform = dict()
    for root, dirs, files in os.walk(path):
        for file in files:
            with open(root + file, 'r') as fp:
                plattform, collection, _ = file.split('.')[0].split('-')
                p = plattform + '-' + collection
                if p not in reverse_mapping:
                    reverse_mapping[p] = dict()
                all_per_plattform[p] = set()
                for line in fp:
                    vlid, sys_number = line.split(',')
                    if sys_number in full_mapping:
                        raise Exception('Found a duplicate system number: {}!!!!!!'.format(sys_number))
                    else:
                        sys_number = sys_number.strip()
                        full_mapping[sys_number] = dict()
                        full_mapping[sys_number]['path'] = p
                        full_mapping[sys_number]['vlids'] = [vlid]
                        reverse_mapping[p][vlid] = sys_number
                        all_per_plattform[p].add(int(vlid))
    return full_mapping, all_per_plattform, reverse_mapping


if __name__ == '__main__':
    result = dict()
    mapping, title_vlids_per_plattform, reverse_mapping = get_mapping_vlids_sys_num('mapping/')
    vlids, vlids_per_plattform = get_vlids('data/')

    for p in ['emanus-bau', 'emanus-swa']:
        for vlid in vlids_per_plattform[p]:
            if vlid in title_vlids_per_plattform[p]:
                pass
            else:
                temp = list()
                for v in title_vlids_per_plattform[p]:
                    if v < vlid:
                        temp.append(v)
                if len(temp) > 0:
                    reference_vlid = max(temp)
                    sys_number = reverse_mapping[p][str(reference_vlid)]
                    mapping[sys_number]['vlids'].append(vlid)

    for sys_number in mapping:
        p = mapping[sys_number]['path']

        for v in mapping[sys_number]['vlids']:
            for y in ['2016', '2017', '2018']:
                if sys_number not in result:
                    result[sys_number] = dict()
                if p not in result[sys_number]:
                    result[sys_number][p] = dict()
                if v in vlids[p][y]:
                    if y not in result[sys_number]:
                        result[sys_number][p][y] = vlids[p][y][v]['page-views']
                    else:
                        result[sys_number][p][y] += vlids[p][y][v]['page-views']
                else:
                    if y not in result[sys_number][p]:
                        result[sys_number][p][y] = 0

    elastic_data = list()
    for sys_number in result:
        item = result[sys_number]
        total = 0
        for year in item[list(item.keys())[0]]:
            total += item[list(item.keys())[0]][year]
        item['total'] = total
        item['identifier'] = sys_number
        elastic_data.append(item)

    index = ElasticIndex('e-emanuscripta-data', 'hits')
    index.bulk(elastic_data, 'identifier')

