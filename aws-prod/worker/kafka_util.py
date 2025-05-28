import logging
import json
import boto3
from kafka import KafkaProducer, KafkaConsumer
from config import REGION
from config import KAFKA_ADDRESS
from logger_util import logger

import atexit

class KafkaProducerSingleton:
    _producer = None

    @staticmethod
    def get_producer():
        """Returns a singleton Kafka producer instance."""
        try:
            if KafkaProducerSingleton._producer is None:
            # kafka_address = get_kafka_address(REGION) # Un comment on AWS
                kafka_address = KAFKA_ADDRESS  # Comment on AWS
                KafkaProducerSingleton._producer = KafkaProducer(

                    bootstrap_servers=[kafka_address],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logger.info("Kafka producer created successfully")

            return KafkaProducerSingleton._producer
        except Exception as e:
            logger.error(f"Error creating Kafka producer: {e}")
            raise

    @staticmethod
    def close_producer():
        """Flush and close the Kafka producer at application exit."""
        if KafkaProducerSingleton._producer is not None:
            KafkaProducerSingleton._producer.flush()
            KafkaProducerSingleton._producer.close()
            KafkaProducerSingleton._producer = None  # Mark as closed

    @staticmethod
    def get_kafka_address(self, region_name):
        client = boto3.client('ssm', region_name=region_name)
        response = client.get_parameter(Name='/kafka/public-address')
        print(response)
        return response['Parameter']['Value']

def create_kafka_consumer(topic,group_id='master_group'):
    """Create and return a Kafka consumer for the task_results topic."""
    try:
        # kafka_address = get_kafka_address(REGION) # Un comment on AWS
        kafka_address = KAFKA_ADDRESS               # Comment on AWS
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_address],
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise


atexit.register(KafkaProducerSingleton.close_producer)



