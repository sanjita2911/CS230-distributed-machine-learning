import json
import boto3
from kafka import KafkaProducer, KafkaConsumer
from config import REGION
from config import KAFKA_ADDRESS,KAFKA_TRAIN_TOPIC,KAFKA_RESULTS_TOPIC
from logger_util import logger

from kafka import KafkaProducer
import atexit


def send_to_kafka(subtasks, producer):
    """
    send individual tasks to Kafka.

    Args:
        job_id (str): Unique job identifier
        session_id (str): Session identifier
        dataset_id (str): Dataset identifier
        timestamp (str): Timestamp when the task was created
        subtasks (list): List of subtasks to be distributed
        producer:
    """

    # Initialize Kafka producer
    # producer = KafkaProducer(
    #     bootstrap_servers=['localhost:9092'],
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )

    # Send each subtask to Kafka topic
    for subtask in subtasks:
        try:
            # Send to a single topic for all tasks
            producer.send(KAFKA_TRAIN_TOPIC, subtask)
        except Exception as e:
            logger.error(f"Error sending task to Kafka: {str(e)}")

    # Make sure all messages are sent
    producer.flush()

class KafkaSingleton:
    _producer = None
    _consumer = None

    @staticmethod
    def get_producer():
        """Returns a singleton Kafka producer instance."""
        try:
            if KafkaSingleton._producer is None:
            # kafka_address = get_kafka_address(REGION) # Un comment on AWS
                kafka_address = KAFKA_ADDRESS  # Comment on AWS
                KafkaSingleton._producer = KafkaProducer(

                    bootstrap_servers=[kafka_address],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logger.info("Kafka producer created successfully")

            return KafkaSingleton._producer
        except Exception as e:
            logger.error(f"Error creating Kafka producer: {e}")
            raise

    @staticmethod
    def get_consumer():
        """Returns a singleton Kafka consumer instance."""
        try:
            if KafkaSingleton._consumer is None:

                # kafka_address = get_kafka_address(REGION) # Un comment on AWS
                kafka_address = KAFKA_ADDRESS  # Comment on AWS
                KafkaSingleton._consumer = KafkaConsumer(
                    KAFKA_RESULTS_TOPIC,
                    bootstrap_servers=[kafka_address],
                    auto_offset_reset='earliest',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    enable_auto_commit=True
                )
                logger.info("Kafka consumer created successfully")

            return KafkaSingleton._consumer
        except Exception as e:
            logger.error(f"Error creating Kafka consumer: {e}")
            raise


    @staticmethod
    def close_producer():
        """Flush and close the Kafka producer at application exit."""
        if KafkaSingleton._producer is not None:
            KafkaSingleton._producer.flush()
            KafkaSingleton._producer.close()
            KafkaSingleton._producer = None  # Mark as closed

    @staticmethod
    def close_consumer():
        """Flush and close the Kafka producer at application exit."""
        if KafkaSingleton._consumer is not None:
            KafkaSingleton._consumer.close()
            KafkaSingleton._consumer = None  # Mark as closed

    @staticmethod
    def get_kafka_address(self, region_name):
        client = boto3.client('ssm', region_name=region_name)
        response = client.get_parameter(Name='/kafka/public-address')
        print(response)
        return response['Parameter']['Value']



atexit.register(KafkaSingleton.close_consumer)
atexit.register(KafkaSingleton.close_producer)



