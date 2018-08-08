from simple_elastic import ElasticIndex

import re
import logging
import sys

if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

    with open('data/VERZ_DSV01_Vollanzeige.txt', 'r') as file:
        index_objects = list()
        identifier = 0

        count = 0

        for line in file:
            match = re.match('access_log(_ssl)?.*:(?P<ip>[\d.]+) - - \[(?P<timestamp>.*)\] "(?P<method>\w+) (.*)?'
                             '\/F\/(?P<session>.*)-\d+\?(.*)?doc_number(=|%3D)(?P<system_number>\d+).*$', line)
            index_object = None
            if match:
                captures = match.groupdict()

                index_object = captures
                index_object['identifier'] = identifier
                identifier += 1

                date = re.match('(?P<day>\d+)\/(?P<month>\w+)\/(?P<year>\d+):'
                                '(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+) .*$', captures['timestamp'])
                if date:
                    dates = date.groupdict()
                    index_object['date_parts'] = dates
                else:
                    logging.warning('Could not parse date time in line "%s".', line)
            else:
                logging.warning('Could not parse line "%s".', line)

            if index_object is not None:
                index_objects.append(index_object)

            count += 1

            if count == 500:
                index = ElasticIndex('opac-access', 'log')
                index.bulk(index_objects, identifier_key='identifier')
                count = 0
                index_objects.clear()

        index = ElasticIndex('opac-access', 'log')
        index.bulk(index_objects, identifier_key='identifier')

