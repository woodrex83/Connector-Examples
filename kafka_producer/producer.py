"""
Confluent Kafka Producer
Example can be found at confluent-kafka-python git repo

Example usage:

    p = KafkaProducer(settings.kafka)
    for _ in range(100000):
        p.send(topic, payload)
    p._producer.flush()

Patrick Mok
10/3/2024
"""
from confluent_kafka import Producer
from loguru import logger

from kafka_producer.setting import KafkaSettings


class KafkaProducer:
    def __init__(self, kafka_settings: KafkaSettings):
        self._producer = Producer({
            'bootstrap.servers': kafka_settings.bootstrap_server,
            'security.protocol': kafka_settings.security_protocol,
            'ssl.ca.location': kafka_settings.ssl_cafile,
            'ssl.certificate.location': kafka_settings.ssl_certfile,
            'ssl.key.location': kafka_settings.ssl_keyfile
            # 'enable.ssl.certificate.verification': False
        })
        self._topic = kafka_settings.topic
    
    def delivery_callback(self, error, message) -> None:
        if error:
            logger.error(f"Message failed delivery due to : {error}")
        else:
            # Efficiency drop if using loguru instead of print
            # logger.success(f"Message delivered to {message.topic()}")
            print(f"Message delivered to {message.topic()}")
        
    def send(self, message: str) -> None:
        self._producer.produce(
            topic=self._topic,
            value=message.encode("utf-8"),
            on_delivery=self.delivery_callback
        )
        self._producer.poll(0)
