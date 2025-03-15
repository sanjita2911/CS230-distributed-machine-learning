import redis
from logger_util import logger
import boto3
from config import REGION,REDIS_ADDRESS
from datetime import datetime
import json

def create_redis_client():
    """Create and return a Redis client."""
    try:
        client = redis.Redis(host=REDIS_ADDRESS, port=6379, decode_responses=True)
        logger.info("Redis client created successfully")
        return client
    except Exception as e:
        logger.error(f"Error creating Redis client: {e}")
        raise


def save_subtasks_to_redis(subtasks, redis_client):
    """
    Save subtasks to Redis under session_id -> job_id -> subtasks structure.

    Args:
        subtasks (list): List of subtask configurations
        redis_config (dict): Redis connection configuration

    Returns:
        dict: Result of the operation
    """
    if not subtasks:
        return {"status": "error", "message": "No subtasks to save"}

    # Default Redis configuration


    try:
        # Connect to Redis

        # Get session_id and job_id from the first subtask
        session_id = subtasks[0]['session_id']
        job_id = subtasks[0]['job_id']

        # Create key structure
        session_key = f"active_sessions:{session_id}"
        job_key = f"{session_key}:jobs:{job_id}"

        # Store job metadata
        redis_client.hset(job_key, "total_subtasks", len(subtasks))
        redis_client.hset(job_key, "completed_subtasks", 0)
        redis_client.hset(job_key, "status", "pending")
        redis_client.hset(job_key, "created_at", datetime.now().isoformat())

        # Add job to session list
        redis_client.sadd(f"{session_key}:jobs", job_id)

        # Save each subtask
        for subtask in subtasks:
            subtask_id = subtask['subtask_id']
            subtask_key = f"{job_key}:subtasks:{subtask_id}"

            # Convert subtask to JSON
            subtask_json = json.dumps(subtask, default=str)

            # Store subtask
            redis_client.set(subtask_key, subtask_json)

            # Add to pending subtasks list
            redis_client.rpush(f"{job_key}:pending_subtasks", subtask_id)

            # Add to subtasks set
            redis_client.sadd(f"{job_key}:subtasks", subtask_id)

        return {
            "status": "success",
            "message": f"Saved {len(subtasks)} subtasks to Redis",
            "session_id": session_id,
            "job_id": job_id
        }

    except redis.RedisError as e:
        return {"status": "error", "message": f"Redis error: {str(e)}"}

    except Exception as e:
        return {"status": "error", "message": f"Error saving to Redis: {str(e)}"}


def update_subtask(session_id, job_id, subtask_id, redis_client):
    """
    Mark a subtask as completed in Redis and update job progress.

    Args:
        session_id (str): The session ID.
        job_id (str): The job ID.
        subtask_id (str): The subtask ID.
        redis_client (redis.Redis): Redis client instance.

    Returns:
        dict: Result of the update operation.
    """
    try:
        # Define Redis key structure
        session_key = f"ml:sessions:{session_id}"
        job_key = f"{session_key}:jobs:{job_id}"
        subtask_key = f"{job_key}:subtasks:{subtask_id}"

        # Check if subtask exists
        if not redis_client.exists(subtask_key):
            return {"status": "error", "message": f"Subtask {subtask_id} not found"}

        # Update subtask status to completed
        subtask_data = json.loads(redis_client.get(subtask_key))
        subtask_data["status"] = "completed"
        subtask_data["completed_at"] = datetime.now().isoformat()
        redis_client.set(subtask_key, json.dumps(subtask_data))

        # Remove subtask from pending list and add to completed list
        redis_client.lrem(f"{job_key}:pending_subtasks", 0, subtask_id)
        redis_client.sadd(f"{job_key}:completed_subtasks", subtask_id)

        # Update completed subtasks count
        redis_client.hincrby(job_key, "completed_subtasks", 1)

        # Check if all subtasks are completed
        total_subtasks = int(redis_client.hget(job_key, "total_subtasks") or 0)
        completed_subtasks = int(redis_client.hget(job_key, "completed_subtasks") or 0)

        if completed_subtasks >= total_subtasks:
            redis_client.hset(job_key, "status", "completed")
            redis_client.hset(job_key, "completed_at", datetime.now().isoformat())

        return {
            "status": "success",
            "message": f"Subtask {subtask_id} marked as completed",
            "session_id": session_id,
            "job_id": job_id,
            "subtask_id": subtask_id
        }

    except redis.RedisError as e:
        return {"status": "error", "message": f"Redis error: {str(e)}"}

    except Exception as e:
        return {"status": "error", "message": f"Error updating Redis: {str(e)}"}
