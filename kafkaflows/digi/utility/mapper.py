from isodate.isoerror import ISO8601Error
import isodate

import logging
import re

class MARCMapper(object):

    def __init__(self, record, logger=logging.getLogger(__name__)):
        self._record = record
        self.logger = logger
        self.result = dict()

        self.result['error_tags'] = list()
        self.result['identifiers'] = dict()
        self.result['extent'] = dict()
        self.result['dates'] = dict()
        self.result['production'] = dict()

    def add_value(self, tag, value):
        self.result[tag] = value

    def add_value_sub(self, tag, field, value):
        if tag not in self.result:
            self.result[tag] = dict()
        self.result[tag][field] = value

    def add_value_sub_sub(self, tag, sub_tag, sub_sub_tag, value):
        if tag not in self.result:
            self.result[tag] = dict()
        if sub_tag not in self.result[tag]:
            self.result[tag][sub_tag] = dict()
        self.result[tag][sub_tag][sub_sub_tag] = value

    def append_value_sub(self, tag, field, value):
        if tag not in self.result:
            self.result[tag] = dict()
        if field not in self.result[tag]:
            self.result[tag][field] = list()
        self.result[tag][field].append(value)

    def identifier(self, tag='identifier'):
        self.result[tag] = self._record['001'].value()

    def add_identifier(self, tag, value):
        self.result['identifiers'][tag] = value

    def add_error_tag(self, tag):
        self.result['error_tags'].append(tag)

    def parse_leader(self):
        self.result['leader'] = dict()
        self.result['leader']['record_length'] = self._record.leader[:4]
        self.result['leader']['record_status'] = self._record.leader[5]
        self.result['leader']['type_of_record'] = self._record.leader[6]
        self.result['leader']['bibliographic_level'] = self._record.leader[7]
        self.result['leader']['type_of_control'] = self._record.leader[8]

    def parse_cat_date(self):
        _008 = self._record['008'].value()
        if re.match('\d+', _008[:6]):
            self.result['dates']['cat'] = dict()
            self.result['dates']['cat']['year'] = '20' + _008[:2] if int(_008[:2]) != 99 else '19' + _008[:2]
            self.result['dates']['cat']['month'] = _008[2:4]
            self.result['dates']['cat']['day'] = _008[4:6]
            self.result['dates']['cat']['date'] = self.result['dates']['cat']['year'] + '-' + \
                                                  self.result['dates']['cat']['month'] + '-' + \
                                                  self.result['dates']['cat']['day']
            try:
                isodate.parse_date(self.result['dates']['cat']['date'])
            except (ISO8601Error, ValueError):
                self.add_error_tag('_could_not_parse_catalogue_date')
                self.result['dates']['cat']['date'] = '1999-11-30'
        else:
            self.logger.warning('Could not parse cat date: %s for record %s.', _008[:6], self.result['identifier'])

    def parse_date_from_008(self) -> bool:
        _008 = self._record['008'].value()
        self.result['dates']['date'] = dict()
        self.result['dates']['date']['type'] = _008[6]
        if _008[6] == 'n':
            self.logger.info('No date defined in field 008 (code n)')
            return False

        if _008[6] not in ['s', 'm', 'q', 'i', 'd', 'r']:
            self.logger.error('Unexpected value in field 008 F6 (date type): %s.', _008[6])
            return False

        year_1 = _008[7:11]
        year_2 = _008[11:15]

        if re.match('\d{4}', year_1):
            year_1 = int(year_1)
        else:
            self.logger.info('Invalid date in 008 F7-11: %s', year_1)
            return False

        if _008[6] == 's':
            self.result['dates']['date']['year'] = year_1
            return True

        if re.match('\d{4}', year_2):
            year_2 = int(year_2)

        if _008[6] == 'r':
            self.result['dates']['date']['year'] = year_1
            if isinstance(year_2, int):
                self.result['dates']['date']['original'] = year_2
            return True

        if _008[6] in ['m', 'q', 'i', 'd']:
            self.result['dates']['date']['year'] = year_1
            if isinstance(year_2, int):
                self.result['dates']['date']['to'] = year_2
            return True

    def parse_date_from_046(self) -> bool:
        if self._record['046'] is not None:
            self.result['dates']['exact'] = dict()
            if self._record['046']['a'] is not None:
                code = self._record['046']['a']
                self.result['dates']['exact']['type'] = code
                if code in ['n', 'und']:
                    self.add_error_tag('_no_valid_046_date')
                    return False
                elif code in ['s']:
                    if self._record['046']['c'] is not None:
                        date = self._record['046']['c']
                        if re.match('\d{4}$', date):
                            self.result['dates']['exact']['year'] = int(date)
                            return True
                        elif re.match('\d{4}\.\d{2}\.\d{2}$', date):
                            year, month, day = date.split('.')
                            self.result['dates']['exact']['year'] = int(year)
                            self.result['dates']['exact']['month'] = int(month)
                            self.result['dates']['exact']['day'] = int(day)
                            return True
                        else:
                            self.add_error_tag('_no_valid_046_date')
                            return False
                elif code in ['q', 'm']:
                    if self._record['046']['c'] is not None:
                        date = self._record['046']['c']
                        if re.match('\d{4}$', date):
                            self.result['dates']['exact']['year'] = int(date)
                        elif re.match('\d{4}\.\d{2}\.\d{2}$', date):
                            year, month, day = date.split('.')
                            self.result['dates']['exact']['year'] = int(year)
                            self.result['dates']['exact']['month'] = int(month)
                            self.result['dates']['exact']['day'] = int(day)
                        else:
                            self.add_error_tag('_no_valid_046_date')
                    if self._record['046']['e'] is not None:
                        self.result['dates']['exact']['to'] = self._record['046']['e']
                    if self._record['046']['b'] is not None:
                        self.result['dates']['exact']['from_b_Chr'] = self._record['046']['b']

                    if '_no_valid_046_date' in self.result['error_tags']:
                        return False
                    else:
                        return True
        else:
            return False

    def parse_date_from_264(self) -> bool:
        if self._record['264'] is not None:
            if self._record['264']['c'] is not None:
                date = re.search('(?P<year>\d{4})', self._record['264']['c'])
                if date is not None:
                    self.result['dates']['parsed_264_year'] = int(date.groupdict()['year'])
                    return True
                else:
                    return False
            else:
                return False
        else:
            return False

    def parse_rest_008(self):
        _008 = self._record['008'].value()
        self.result['production']['country_code'] = _008[15:17]
        self.result['production']['lang_code'] = _008[35:38]

    def parse_field(self, field, subfield, tag):
        if self._record[field] is not None:
            if self._record[field][subfield] is not None:
                self.result[tag] = self._record[field][subfield]

    def parse_field_to_subfield(self, field_tag, subfield_tag, tag, sub_tag):
        if self._record[field_tag] is not None:
            if self._record[field_tag][subfield_tag] is not None:
                if tag not in self.result:
                    self.result[tag] = dict()
                self.result[tag][sub_tag] = self._record[field_tag][subfield_tag]

    def parse_field_append_to_subfield(self, field_tag, subfield_tag, tag, sub_tag):
        for field in self._record.get_fields(field_tag):
            if field[subfield_tag] is not None:
                if tag not in self.result:
                    self.result[tag] = dict()
                if sub_tag not in self.result[tag]:
                    self.result[tag][sub_tag] = list()
                self.result[tag][sub_tag].append(field[subfield_tag])

    def parse_field_list(self, fields, subfield_map, tag):
        if tag not in self.result:
            self.result[tag] = list()
        for field_name in fields:
            for field in self._record.get_fields(field_name):
                entry = dict()
                for key in subfield_map:
                    if field[key] is not None:
                        entry[subfield_map[key]] = field[key]
                self.result[tag].append(entry)

    def get_fields(self, tag):
        return self._record.get_fields(tag)

    def __getitem__(self, item):
        return self._record[item]