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

def get_metadata(subtask_id, redis_client):
    """
    Quickly fetch job metadata using subtask_id via direct mapping.

    Args:
        subtask_id (str): ID of the subtask.
        redis_client (redis.Redis): Redis client.

    Returns:
        dict: Metadata or error.
    """
    try:
        mapping_key = f"subtask_to_metadata:{subtask_id}"
        metadata_key = redis_client.get(mapping_key)
        if not metadata_key:
            return {"status": "error", "message": f"No metadata key mapped for subtask {subtask_id}"}

        metadata = redis_client.hgetall(metadata_key)
        if not metadata:
            return {"status": "error", "message": "No metadata found at referenced key"}

        return metadata
    except Exception as e:
        return {"status": "error", "message": f"Redis error: {e}"}