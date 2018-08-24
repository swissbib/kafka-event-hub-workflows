from simple_elastic import ElasticIndex

import json
import os
import logging
import sys
import re

CONTENT_TYPE_IGNORE_LIST = [
    # content type empty...
    '',
    # stuff for website
    'ajax',
    'renderedajax',
    # admin stuff
    'qa', 'vlm', 'doc', 'admin', 'oai', 'state', 'request',
    # wiki stuff
    'rss', 'wiki', 'i3f',
    # maybe usefull? Search & Download. TODO: Ask what this is.
    'download', 'search', 'nav']

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)

missing_vlid = open('missing_vlid.txt', 'w')

vlid_index = ElasticIndex('vlid_mapping', 'mapping')

for root, dirs, files in os.walk('data/bau_1/'):
    for file in files:
        logging.debug(file)
        index = ElasticIndex(file.split('.')[0], 'record')

        with open(root + file, 'r') as fp:
            data = json.load(fp)

            vlid_collection = dict()
            fixed_data = list()

            for metric in data['data']:
                # do this as there are some spaces in the URL sometimes...
                page_stem_clean = metric['dimensions']['pagestem']
                page_stem_clean = page_stem_clean.strip()
                page_stem_clean = page_stem_clean.replace(' ', '')
                page_stem_clean = page_stem_clean.replace('//', '/')

                # only process links which start with /bau_1/
                if re.match('/(bau_1|bau|swa)/.*', page_stem_clean):
                    item = dict()
                    item['identifier'] = metric['dimensions']['pagestem']
                    item['page'] = page_stem_clean

                    item['page_views'] = metric['metrics']['pageviews']
                    item['unique_page_views'] = metric['metrics']['uniquepageviews']
                    item['exits'] = metric['metrics']['exits']
                    item['landing_page'] = metric['metrics']['landingpage']

                    page_stem = item['page'].split('/')[1:]

                    if file.startswith('erara'):
                        item['source'] = 'e-rara'
                    elif file.startswith('emanus-bau'):
                        item['source'] = 'e-manuscripta/ub'
                    elif file.startswith('emanus-swa'):
                        item['source'] = 'e-manuscripta/swa'
                    if len(page_stem) == 1:
                        item['content_type'] = 'library_information'
                    else:
                        item['content_type'] = page_stem[1]

                    if item['content_type'] in ['content', 'image', 'periodical']:
                        item['view'] = page_stem[2]

                        if item['view'] == 'search':
                            try:
                                item['vlid'], item['search_query'] = page_stem[3].split('?')
                            except ValueError:
                                logging.error(page_stem_clean)
                        else:
                            try:
                                item['vlid'] = page_stem[3]
                            except IndexError:
                                pass
                    # verschiedene Sammlungen. Diese Verweise funktionieren auch ohne den Unterordner...
                    elif item['content_type'] in [
                        # in e-rara
                        'ch', 'ch15', 'ch16', 'ch17', 'ch18', 'ch19', 'ch20',
                        'maps', 'misc', 'music', 'collections', 'wihibe', 'alch', 'flug',
                        # in e-manuscripta
                        'all', 'texte', 'rezepte', 'briefe', 'selected', 'tagebuecher', 'bilder', 'noten', 'platter',
                        'wagner', 'selbstzeugnisse', 'personen', 'sonnenbeobachtung', 'gattungen', 'kataloge',
                        'chroniken', 'transcript'
                         ]:
                        item['collection'] = item['content_type']
                        try:
                            item['content_type'] = page_stem[2]
                        except IndexError:
                            # Values of no interest to me as there is no further information after the collection tag.
                            logging.error('Could not index content type for collection: %s', page_stem_clean)
                            # jump to next item.
                            continue
                        else:
                            try:
                                if item['content_type'] != 'nav':
                                    item['view'] = page_stem[3]
                                    if '?' in page_stem[4]:
                                        item['vlid'], item['search_query'] = page_stem[4].split('?')
                                    else:
                                        item['vlid'] = page_stem[4]
                                else:
                                    item['view'] = page_stem[3]
                                    if item['view'] == 'classification':
                                        if '?' in page_stem[4]:
                                            item['vlid'], item['search_query'] = page_stem[4].split('?')
                                        else:
                                            item['vlid'] = page_stem[4]
                                    else:
                                        item['searched_field'], item['search_query'] = page_stem[4].split('?')
                            except (IndexError, ValueError):
                                logging.error('Could not index pagestem: %s', page_stem_clean)
                                continue
                    # genereller direkter verweis auf die VLID mit im format www.e-rara.ch/bau_1/id/<vlid>
                    elif item['content_type'] == 'id':
                        item['content_type'] = 'title_page'
                        if '?' in page_stem[2]:
                            item['vlid'], item['search_query'] = page_stem[2].split('?')
                        else:
                            item['vlid'] = page_stem[2]
                    # Zugriff über den DOI.
                    elif item['content_type'] == 'doi':
                        item['doi'] = page_stem[2] + '/' + page_stem[3]
                    # Zugriff auf das Thumbnail über den DOI.
                    elif item['content_type'] == 'titlepage':
                        item['content_type'] = 'title_page_thumbnail_128px'
                        item['doi'] = page_stem[3] + '/' + page_stem[4]
                        item['image_size'] = page_stem[5]
                    else:
                        try:
                            item['vlid'] = str(int(item['content_type']))
                            item['content_type'] = 'title_page'
                        except ValueError:
                            if item['content_type'] not in CONTENT_TYPE_IGNORE_LIST:
                                logging.warning('No indexing for item with CT %s with pagestem %s', item['content_type']
                                                , page_stem_clean)

                    if 'vlid' in item:
                        v = item['vlid']
                        try:
                            x = int(v)
                        except ValueError:
                            print(v, item)
                        if v != '':
                            if v not in vlid_collection:
                                vlid_collection[v] = dict()
                                vlid_collection[v]['vlid'] = v
                                vlid_collection[v]['page_views'] = item['page_views']
                                vlid_collection[v]['unique_page_views'] = item['unique_page_views']
                                vlid_collection[v]['exits'] = item['exits']
                                vlid_collection[v]['landing_page'] = item['landing_page']
                            else:
                                vlid_collection[v]['page_views'] += item['page_views']
                                vlid_collection[v]['unique_page_views'] += item['unique_page_views']
                                vlid_collection[v]['exits'] += item['exits']
                                vlid_collection[v]['landing_page'] += item['landing_page']

                            try:
                                item['system_number'] = vlid_collection[v]['system_number'] = vlid_index.get(item['vlid'])['idn']
                            except TypeError:
                                missing_vlid.write(item['vlid'] + '|' + page_stem_clean + '\n')
                        else:
                            logging.error('Empty vlid for pagestem %s.', page_stem_clean)
                            del item['vlid']

                    # ignore certain special content types. These are for admin, website related information or
                    # display related and do not reflect any access to an actual item.
                    if item['content_type'] not in CONTENT_TYPE_IGNORE_LIST:
                        fixed_data.append(item)

        index.bulk(fixed_data, 'identifier')

        index_2 = ElasticIndex('vlid-' + file.split('.')[0], doc_type='record')
        vlid_list = [vlid_collection[i] for i in vlid_collection]
        index_2.bulk(vlid_list, 'vlid')

        logging.error('Indexed %s', file)



