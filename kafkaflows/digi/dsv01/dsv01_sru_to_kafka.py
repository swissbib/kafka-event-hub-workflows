from kafka_event_hub.producers import SRUProducer
from kafka_event_hub.config.event_hub_config import BaseConfig
from simple_elastic import ElasticIndex


if __name__ == '__main__':
    dsv01_arc = ElasticIndex('dsv01-full-arc-export', 'record')

    for element in dsv01_arc.scroll():
        for item in element:
            if item['system_number'] != '000013825':
                producer = SRUProducer(BaseConfig('config/dsv01_dump.yml'))
                producer.query_id_equal(item['system_number'])
                producer.process()



