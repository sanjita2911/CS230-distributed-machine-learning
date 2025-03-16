import json
from redis_util import create_redis_client
import uuid
from kafka import KafkaProducer, KafkaConsumer

from config import KAFKA_RESULTS_TOPIC,KAFKA_TRAIN_TOPIC,KAFKA_ADDRESS
from datetime import datetime
from sklearn.model_selection import ParameterGrid,ParameterSampler
from logger_util import logger
import threading
from kafka_util import KafkaSingleton
from kafka import KafkaConsumer
import json
from flask import jsonify
from config import KAFKA_RESULTS_TOPIC, KAFKA_ADDRESS
from redis_util import create_redis_client,update_subtask
import time





def start_result_collector():
    """Start a background thread to collect results from Kafka"""
    result_thread = threading.Thread(target=consume_results, daemon=False)
    result_thread.start()
    logger.info("Result collector thread started...")


def consume_results(subtasks,session_id):
    """Consume results from Kafka and update Redis"""
    redis_client = create_redis_client()
    try:
        logger.info("Initializing Kafka consumer...")
        consumer = KafkaSingleton.get_consumer()
        # consumer = KafkaConsumer(
        #     KAFKA_RESULTS_TOPIC,
        #     bootstrap_servers=['127.0.0.1:9092'],
        #     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        #     auto_offset_reset='earliest',
        #     enable_auto_commit=False,
        #     group_id="master_result_collector"  # Add a consumer group ID
        # )

        logger.info(f"Kafka consumer successfully connected to {KAFKA_ADDRESS}")
        logger.info(f"Kafka consumer listening to topic: {KAFKA_RESULTS_TOPIC}")

        for message in consumer:
            try:
                if message.value.get('subtask_id') in subtasks:
                    logger.info(f"Received result message: {message.value}")
                    result = message.value
                    process_subtask_result(result,session_id,redis_client)
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}", exc_info=True)
                continue  # Continue to the next message even if this one failed
    except Exception as e:
        logger.error(f"Fatal error in Kafka consumer: {str(e)}", exc_info=True)
        # Sleep for a bit before potentially trying to reconnect
        # time.sleep(5)
        # logger.info("Attempting to restart consumer...")
        # consume_results()  #

def process_subtask_result(result,session_id,redis_client):
    """Process a subtask result and update Redis"""
    subtask_id = result.get('subtask_id')
    job_id = result.get('job_id')
    status = result.get('status')

    if not all([subtask_id, job_id, status]):
        logger.error(f"Invalid result format: {result}")
        return

    # Update subtask status in Redis

    status = update_subtask(session_id, job_id, subtask_id, status, result, redis_client)

    logger.info(status)
    # Check if all subtasks for this job are complete
    check_job_completion(job_id,session_id,redis_client)


def check_job_completion(job_id,session_id,redis_client):
    """Check if all subtasks for a job are complete"""
    # Get all subtasks for this job
    session_key = f"active_sessions:{session_id}"
    job_key = f"{session_key}:jobs:{job_id}"
    subtask_keys = redis_client.keys(f"{job_key}:subtasks:{job_id}-subtask-*")

    if not subtask_keys:
        logger.warning(f"No subtasks found for job {job_id}")
        return

    total_subtasks = len(subtask_keys)
    completed_subtasks = 0
    aggregated_results = []

    # Check status of each subtask
    for key in subtask_keys:
        subtask_data = redis_client.get(key)
        if subtask_data:
            subtask = json.loads(subtask_data)
            if subtask.get('status') == 'completed':
                completed_subtasks += 1
                # Collect results for aggregation
                if 'result' in subtask:
                    aggregated_results.append(subtask['result'])

    # Update job completion percentage
    completion_percentage = (completed_subtasks / total_subtasks) * 100
    redis_client.hset(job_key, "status", str(completion_percentage))

    # If all subtasks are complete, mark job as complete
    if completed_subtasks == total_subtasks:
        job_status = redis_client.hget(job_key,"status")
        logger.info(f"Job {job_id} completion status : {job_status} %. All {total_subtasks} subtasks finished.")
        # Process aggregated results

        best_result = aggregate_results(aggregated_results)
        final_result = {
            'results': aggregated_results,
            'best_result': best_result,
            'completion_time': datetime.now().isoformat()
        }
        redis_client.set(f"{job_key}:result", json.dumps(final_result))
        redis_client.hset(job_key, "status", "completed")
        job_status = redis_client.hget(f"{job_key}","status")
        logger.info(job_status)
        # Decrement pending tasks counter for the associated session
        # session_id = redis_client.get(f"jobs:{job_id}:session")
        # if session_id:
        #     session_id = session_id.decode('utf-8')
        #     pending_tasks_key = f"active_sessions:{session_id}:tasks_pending"
        #     redis_client.decr(pending_tasks_key)
        #     logger.info(f"Decremented pending tasks for session {session_id}")


# # Add this to update_subtask function in redis_util.py
# def update_subtask(subtask_id, session_id, job_id, status, result_data, redis_client):
#     """Update a subtask in Redis with new status and result data"""
#     session_key = f"active_sessions:{session_id}"
#     job_key = f"{session_key}:jobs:{job_id}"
#     logger.info(job_key)
#     subtask_key = redis_client.keys(f"{job_key}:subtasks:{subtask_id}")[0]
#     logger.info(subtask_key)
#     pending_subtasks_key = f"{job_key}:pending_subtasks"
#     # Get existing subtask data
#     existing_data = redis_client.get(subtask_key)
#     if existing_data:
#         subtask = json.loads(existing_data)
#         logger.info(subtask)
#         # Update with new data
#         subtask['status'] = status
#         subtask['result'] = result_data.get('result', {})
#         subtask['updated_at'] = datetime.now().isoformat()
#
#         # Save updated subtask back to Redis
#         redis_client.set(subtask_key, json.dumps(subtask))
#         redis_client.lrem(pending_subtasks_key, 0, subtask_key)
#         logger.info(f"Updated subtask {subtask_id} with status {status}")
#         return True
#     else:
#         logger.error(f"Subtask {subtask_id} not found in Redis")
#         return False



def create_subtasks(job_config):
    """
    Create subtasks for distributed processing based on the job configuration.

    Args:
        job_config (dict): The job configuration from create_job()

    Returns:
        list: List of subtask configurations
    """
    job_id = job_config['job_id']
    session_id = job_config['session_id']
    dataset_id = job_config['dataset_id']
    model_details = job_config['model_details']
    train_params = job_config.get('train_params', {})

    subtasks = []

    # If this is a search job (GridSearchCV or RandomizedSearchCV)
    if 'search_type' in model_details:
        search_type = model_details['search_type']
        base_model_type = model_details['model_type']
        base_params = model_details['hyperparameters']['base_estimator_params']
        cv_params = model_details['hyperparameters'].get('cv_params', {})
        search_params = model_details['hyperparameters'].get('search_params', {})

        # Get CV value for train/test splitting
        cv = cv_params.get('cv', 5)

        # For GridSearchCV, create one subtask per parameter combination
        if search_type == 'GridSearchCV':
            param_grid = search_params.get('param_grid', {})
            param_combinations = list(ParameterGrid(param_grid))

            for i, params in enumerate(param_combinations):
                # Combine base parameters with this specific combination
                full_params = {**base_params, **params}

                subtask_id = f"{job_id}-subtask-{i + 1}"
                subtask = {
                    'subtask_id': subtask_id,
                    'job_id': job_id,
                    'session_id': session_id,
                    'dataset_id': dataset_id,
                    'model_type': base_model_type,
                    'parameters': full_params,
                    'train_params': {
                        'cv': cv,  # Use CV value for validation
                        **train_params
                    }
                }
                subtasks.append(subtask)

        # For RandomizedSearchCV, create requested number of random combinations
        elif search_type == 'RandomizedSearchCV':
            param_distributions = search_params.get('param_distributions', {})
            n_iter = search_params.get('n_iter', 10)
            random_state = cv_params.get('random_state', 42)

            # Generate random parameter combinations
            param_combinations = list(ParameterSampler(
                param_distributions, n_iter, random_state=random_state))

            for i, params in enumerate(param_combinations):
                # Combine base parameters with this specific combination
                full_params = {**base_params, **params}

                subtask_id = f"{job_id}-subtask-{i + 1}"
                subtask = {
                    'subtask_id': subtask_id,
                    'job_id': job_id,
                    'session_id': session_id,
                    'dataset_id': dataset_id,
                    'model_type': base_model_type,
                    'parameters': full_params,
                    'train_params': {
                        'cv': cv,  # Use CV value for validation
                        **train_params
                    }
                }
                subtasks.append(subtask)

    # For regular estimator, create a single subtask
    else:
        subtask_id = f"{job_id}-subtask-1"
        subtask = {
            'subtask_id': subtask_id,
            'job_id': job_id,
            'session_id': session_id,
            'dataset_id': dataset_id,
            'model_type': model_details['model_type'],
            'parameters': model_details['hyperparameters'],
            'train_params': train_params
        }
        subtasks.append(subtask)

    return subtasks

def aggregate_results(aggregated_results):

    # Find the best model
        # Sort by mean CV score for both classification and regression
        sorted_results = sorted(aggregated_results,
                                key=lambda x: x.get('mean_cv_score', 0),
                                reverse=True)

        best_result= sorted_results[0] if sorted_results else None
        return best_result



if __name__=="__main__":

    start_result_collector()




