from loguru import logger

from confluent_kafka import KafkaException
from kafka_producer.producer import KafkaProducer
from kafka_producer.setting import settings

def main():
    payload = '''
    {
        "deviceProfileName":"AM103",
        "deviceProfileID":""
    }
    '''

    try:
        p = KafkaProducer(settings.kafka)
        for _ in range(100000):
            p.send(payload)
        p._producer.flush()

    except KafkaException as e:
        logger.error(e)

if __name__ == '__main__':
    main()
