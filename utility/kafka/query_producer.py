# Fetch list object from API and publish to Kafka
import json
import logging
import time
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.INFO)

class Producer:
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def publish(self, topic: str, data: dict):
        self.producer.send(topic, value=data)
        self.producer.flush()
        self.logger.info(f"Published {data} to {topic}")

