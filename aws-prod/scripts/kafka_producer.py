#!/usr/bin/env python3
# kafka_producer.py

from kafka import KafkaProducer
import json
import time
import random
import datetime
import socket
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_hostname():
    """Get the hostname of the current machine."""
    return socket.gethostname()

def generate_event():
    """Generate a sample event with random data."""
    event_types = ["click", "view", "purchase", "login", "logout"]
    users = ["user1", "user2", "user3", "user4", "user5"]
    
    event = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": random.choice(event_types),
        "user_id": random.choice(users),
        "value": round(random.uniform(1.0, 100.0), 2),
        "source_host": get_hostname()
    }
    return event

def delivery_report(err, msg):
    """Callback function to log successful or failed delivery."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    # Replace with your Kafka broker EC2 instance address
    kafka_broker = "YOUR_KAFKA_EC2_PUBLIC_DNS:9092"
    topic = "test-topic"
    
    try:
        # Create producer instance
        producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks='all',
            retries=3,
            request_timeout_ms=15000
        )
        
        logger.info(f"Connected to Kafka broker at {kafka_broker}")
        
        # Send events in a loop
        count = 1
        while True:
            event = generate_event()
            logger.info(f"Sending event #{count}: {event}")
            
            future = producer.send(topic, value=event)
            future.add_callback(lambda metadata: logger.info(
                f"Event sent to partition {metadata.partition}, offset {metadata.offset}"
            ))
            future.add_errback(lambda e: logger.error(f"Error sending event: {e}"))
            
            producer.flush()
            count += 1
            time.sleep(2)  # Send an event every 2 seconds
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    main()