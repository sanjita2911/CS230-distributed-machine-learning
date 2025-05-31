import redis
from logger_util import logger
from config import REGION,REDIS_ADDRESS


def create_redis_client():
    """Create and return a Redis client."""
    try:
        client = redis.Redis(host=REDIS_ADDRESS, port=6379, decode_responses=True)
        logger.info("Redis client created successfully")
        return client
    except Exception as e:
        logger.error(f"Error creating Redis client: {e}")
        raise