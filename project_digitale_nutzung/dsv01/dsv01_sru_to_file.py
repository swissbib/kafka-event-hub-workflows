from kafka_event_hub.producers import SRUProducer
from simple_elastic import ElasticIndex


def process(records):
    result = list()
    for record in records:
        for field in record.get_fields('949'):
            if field['F'] in ['A100', 'A125', 'A130', 'A140']:
                result.append(record)
    return result


if __name__ == '__main__':
    dsv01_arc = ElasticIndex('dsv01-full-arc-export', 'record')

    for element in dsv01_arc.scroll():
        for item in element:
            if item['system_number'] != '000013825':

                producer = SRUProducer()
                producer.process()
                c = Client()
                c.search_anywhere(query='IDSBB' + item['system_number'], store='data/dsv01/', process=process)



