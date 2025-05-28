import json
import boto3
from config import REGION
from config import KAFKA_ADDRESS,KAFKA_TRAIN_TOPIC,KAFKA_RESULTS_TOPIC
from logger_util import logger

from kafka import KafkaProducer,KafkaConsumer
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


def get_kafka_address(region_name):
    client = boto3.client('ssm', region_name=region_name)
    response = client.get_parameter(Name='/kafka/public-address')
    print(response)
    return response['Parameter']['Value']


class KafkaSingleton:
    _producer = None

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
    def close_producer():
        """Flush and close the Kafka producer at application exit."""
        if KafkaSingleton._producer is not None:
            KafkaSingleton._producer.flush()
            KafkaSingleton._producer.close()
            KafkaSingleton._producer = None  # Mark as closed

def get_consumer(topic,group_id=None):
    """Create and return a Kafka consumer for the task_results topic."""
    try:
        # kafka_address = get_kafka_address(REGION) # Un comment on AWS
        kafka_address = KAFKA_ADDRESS               # Comment on AWS
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[kafka_address],
            group_id=group_id,  
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise

atexit.register(KafkaSingleton.close_producer)



