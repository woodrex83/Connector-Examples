import asyncio
from typing import Optional

from kafka_consumer.consumer import KafkaConsumer
from kafka_consumer.setting import settings

async def message_log(message: str, headers: Optional[dict]):
    # Efficiency drop if using loguru instead of print
    # logger.debug(f"Received message: {message}, headers: {headers}")
    print(f"Received message: {message}, headers: {headers}")

async def main():
    try:
        c = KafkaConsumer(settings.kafka)
        async for message, headers in c:
            await message_log(message, headers)
    except KeyboardInterrupt:
        c.close()
        
if __name__ == '__main__':
    asyncio.run(main())