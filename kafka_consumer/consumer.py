"""
Confluent Kafka Consumer
This Kafka consumer is integrated with an asynchronous iterator.

Example usage:
    c = KafkaConsumer(settings.kafka)
    async for message, headers in c:
        # Your custom logic here
        await foo(message, headers)

Patrick Mok
10/3/2024
"""
from confluent_kafka import Consumer, KafkaError
from loguru import logger
from typing import Tuple, Optional, AsyncIterator

from kafka_consumer.setting import KafkaSettings


class KafkaConsumer:
    def __init__(self, kafka_settings: KafkaSettings):
        bootstrap_servers = ','.join(kafka_settings.bootstrap_servers)
        self._consumer = Consumer({
            'group.id': kafka_settings.group_id,
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': kafka_settings.security_protocol,
            'ssl.ca.location': kafka_settings.ssl_cafile,
            'ssl.certificate.location': kafka_settings.ssl_certfile,
            'ssl.key.location': kafka_settings.ssl_keyfile
            # 'enable.ssl.certificate.verification': kafka_settings.ssl_check_hostname
        })
        self._consumer.subscribe(kafka_settings.topics)
    
    def close(self) -> None:
        return self._consumer.close()
    
    def __aiter__(self) -> AsyncIterator[Tuple[str, dict]]:
        return self
    
    async def __anext__(self, timeout: float = 1.0) -> Optional[Tuple[str, dict]]:
        while True:
            message = self._consumer.poll(timeout=timeout)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error("Consumer error: {}".format(message.error()))
                    raise StopAsyncIteration
            
            decoded_message = message.value().decode('utf-8')
            headers = message.headers()
            
            return decoded_message, headers
            
            