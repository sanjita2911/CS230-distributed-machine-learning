import json
import os
import pickle
import time
import numpy as np
import pandas as pd
import glob
import threading
import datetime
import psutil
from kafka_util import create_kafka_consumer, KafkaProducerSingleton
from config import KAFKA_TRAIN_TOPIC, KAFKA_RESULTS_TOPIC, REDIS_ADDRESS
from sklearn.base import BaseEstimator
from sklearn.pipeline import Pipeline
from logger_util import logger
from sklearn.base import is_classifier, is_regressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, r2_score, mean_squared_error
from kafka import KafkaProducer
from redis_util import create_redis_client

r = create_redis_client()
worker_id = None
# HEARTBEAT_INTERVAL = 5
# HEARTBEAT_TTL = HEARTBEAT_INTERVAL * 3
# Dictionary of supported model imports for dynamic loading
MODEL_IMPORTS = {
    # Classifiers
    'RandomForestClassifier': 'from sklearn.ensemble import RandomForestClassifier',
    'LogisticRegression': 'from sklearn.linear_model import LogisticRegression',
    'SVC': 'from sklearn.svm import SVC',
    'GradientBoostingClassifier': 'from sklearn.ensemble import GradientBoostingClassifier',
    'KNeighborsClassifier': 'from sklearn.neighbors import KNeighborsClassifier',

    # Regressors
    'RandomForestRegressor': 'from sklearn.ensemble import RandomForestRegressor',
    'LinearRegression': 'from sklearn.linear_model import LinearRegression',
    'SVR': 'from sklearn.svm import SVR',
    'GradientBoostingRegressor': 'from sklearn.ensemble import GradientBoostingRegressor',
    'KNeighborsRegressor': 'from sklearn.neighbors import KNeighborsRegressor',

    # Transformers
    'StandardScaler': 'from sklearn.preprocessing import StandardScaler',
    'MinMaxScaler': 'from sklearn.preprocessing import MinMaxScaler',
    'PCA': 'from sklearn.decomposition import PCA',
    'OneHotEncoder': 'from sklearn.preprocessing import OneHotEncoder',
    'Imputer': 'from sklearn.impute import SimpleImputer'
}


# def claim_worker_id():
#     while True:
#         # Get the lowest score worker id
#         available = r.zrange("available_worker_ids", 0, 0)
#         if available:
#             worker_id = available[0].decode() if isinstance(
#                 available[0], bytes) else available[0]
#             key = f"worker_claims:{worker_id}"
#             if not r.exists(key):
#                 # Claim it: remove from available, add to active, set heartbeat
#                 r.zrem("available_worker_ids", worker_id)
#                 r.sadd("active_worker_ids", worker_id)
#                 r.set(key, "active", ex=HEARTBEAT_TTL)
#                 logger.info(f"[{worker_id}] Successfully claimed!")
#                 return worker_id
#             else:
#                 logger.info(f"[{worker_id}] Already in use, trying again...")
#         else:
#             logger.info("No available worker IDs, retrying in 5s...")
#             time.sleep(5)


# def heartbeat(worker_id):
#     while True:
#         try:
#             r.set(f"worker_claims:{worker_id}", "active", ex=HEARTBEAT_TTL)
#             print(f"[{worker_id}] Heartbeat")
#         except Exception as e:
#             print(f"[{worker_id}] Heartbeat error: {e}")
#         time.sleep(HEARTBEAT_INTERVAL)


def main():
    """
    Main worker function that consumes ML training tasks from Kafka,
    processes them, and sends results back.
    """
    global worker_id
    # Initialize Redis connection
    # worker_id = claim_worker_id()
    # logger.info(f"Starting worker with ID : {worker_id}.....")
    # threading.Thread(target=heartbeat, args=(worker_id,), daemon=True).start()

    worker_id = os.getenv("WORKER_ID", "unknown")
    logger.info(f"Starting static worker with ID: {worker_id}")
    # Initialize Kafka consumer
    consumer = create_kafka_consumer(KAFKA_TRAIN_TOPIC, group_id='train-group')
    # Initialize Kafka producer for results
    producer = KafkaProducerSingleton.get_producer()

    metrics_producer = KafkaProducer(
        bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logger.info("ML Worker started. Waiting for tasks...")
    # Process messages
    for message in consumer:
        try:
            # Extract task data
            task = message.value
            task_id = task.get('subtask_id')
            job_id = task.get('job_id')

            logger.info(f"Received task: {task_id}")
            logger.info(task)
            # Update task status in Redis
            # update_task_status(task_id, "processing")

            received_at = datetime.datetime.now(datetime.timezone.utc)
            started_at = datetime.datetime.now(datetime.timezone.utc)

            cpu_samples = []
            mem_samples = []
            stop_flag = threading.Event()

            def sampler():
                while not stop_flag.is_set():
                    cpu_samples.append(psutil.cpu_percent(interval=None))
                    mem_samples.append(psutil.virtual_memory().percent)
                    time.sleep(0.5)

            sampler_thread = threading.Thread(target=sampler)
            sampler_thread.start()

            # # Process the task based on its type
            result = process_task(task)

            stop_flag.set()
            sampler_thread.join()
            finished_at = datetime.datetime.now(datetime.timezone.utc)
            cpu_avg = sum(cpu_samples)/len(cpu_samples)
            mem_avg = sum(mem_samples)/len(mem_samples)

            performance_metric = None
            performance_metric_name = None

            if 'accuracy' in result:
                performance_metric = result['accuracy']
                performance_metric_name = 'accuracy'
            elif 'r2_score' in result:
                performance_metric = result['r2_score']
                performance_metric_name = 'r2_score'

            metrics = {
                "worker_id":       worker_id,
                "subtask_id":      task_id,
                "status":          "DONE",
                "received_at":     received_at.isoformat()+"Z",
                "started_at":      started_at.isoformat()+"Z",
                "finished_at":     finished_at.isoformat()+"Z",
                "cpu_percent_avg": cpu_avg,
                "mem_percent_avg": mem_avg,
                "metric_name":     performance_metric_name,
                "metric_value":    performance_metric
            }
            metrics_producer.send('metrics', metrics)
            metrics_producer.flush()
            # Send results to Kafka
            result_message = {
                'subtask_id': task_id,
                'job_id': job_id,
                'status': 'completed',
                'result': result
            }
            producer.send(KAFKA_RESULTS_TOPIC, result_message)
            producer.flush()
            #
            # # Update task status in Redis
            # update_task_status(task_id, "completed", result)  #imp

            # # Commit offset to ensure the message is marked as processed
            consumer.commit()

            logger.info(f"Completed task: {task_id}")

        except Exception as e:
            logger.error(f"Error processing task: {str(e)}")

            # Try to update the task status if we can
            try:
                task_id = task.get('subtask_id')
                job_id = task.get('job_id')

                error_message = {
                    'subtask_id': task_id,
                    'job_id': job_id,
                    'status': 'failed',
                    'error': str(e)
                }
                producer.send(KAFKA_RESULTS_TOPIC, error_message)
                producer.flush()

                # update_task_status(task_id, "failed", {"error": str(e)})

                # Commit offset to move past this message even if it failed
                consumer.commit()
            except:
                logger.error("Failed to report error status")


def train_model(task):
    # Extract task parameters
    algorithm_name = task.get('model_type')
    parameters = task.get('parameters', {})
    # algorithm_type = is_regressor()
    dataset_id = task.get('dataset_id')
    # search_type = task.get('search_type', None)
    train_param = task.get('train_params', {})

    # Load the dataset
    X, y = load_dataset(dataset_id, train_param)

    # Split into train/test sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)

    # Create model instance
    model = get_model_instance(algorithm_name, parameters)
    algorithm_type = check_model_type(model)

    # Train the model
    logger.info(f'Training Started for {algorithm_type}')
    start_time = time.time()
    model.fit(X_train, y_train)
    training_time = time.time() - start_time
    logger.info(f'Training Ended for {algorithm_type}')

    # Evaluate the model
    if algorithm_type == "classifier":
        # Classification metrics
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)

        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='accuracy')

        results = {
            'accuracy': accuracy,
            'cv_scores': cv_scores.tolist(),
            'mean_cv_score': cv_scores.mean(),
            'training_time': training_time
        }
    else:
        # Regression metrics
        y_pred = model.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)

        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')

        results = {
            'r2_score': r2,
            'mean_squared_error': mse,
            'cv_scores': cv_scores.tolist(),
            'mean_cv_score': cv_scores.mean(),
            'training_time': training_time
        }

    # Save the model
    model_id = f"{task.get('subtask_id')}_model"
    model_path = f"./models/{model_id}.pkl"
    os.makedirs('./models', exist_ok=True)
    with open(model_path, 'wb') as file:
        pickle.dump(model, file)

    # Add model path to results
    results['model_id'] = model_id
    results['model_path'] = model_path
    results['parameters'] = parameters

    return results


def process_task(task):
    """
    Process an ML training task.

    Args:
        redis_client: Redis client connection
        task (dict): Task configuration

    Returns:
        dict: Results of the task processing
    """
    # task_type = task.get('task_type')
    #
    # if task_type == "model_training":
    #     return train_model(redis_client, task)
    # elif task_type == "data_preprocessing":
    #     return preprocess_data(redis_client, task)
    # elif task_type == "aggregation":
    #     return aggregate_results(redis_client, task)
    # else:
    #     raise ValueError(f"Unknown task type: {task_type}")
    return train_model(task)


def load_dataset(dataset_id, train_param):
    """
    Load dataset from the data store.

    Args:
        redis_client: Redis client connection
        dataset_id (str): Dataset identifier

    Returns:
        tuple: (X, y) features and target variables
    """
    # In a real implementation, this would fetch from a data store like S3, HDFS, etc.
    # For this example, we'll simulate loading from a file based on dataset_id
    # dataset_path = f"/mnt/efs/datasets/{dataset_id}/{dataset_id}.csv"
    # dataset_path = f"/mnt/datasets/{dataset_id}.csv" # ON AWS uncomment

    data_dir = f"/mnt/efs/datasets/{dataset_id}"
    csv_paths = glob.glob(os.path.join(data_dir, "*.csv"))
    dataset_path = csv_paths[0]

    # if not os.path.exists(dataset_path):  #need to comment on AWS
    #     # Simulate dataset with random data if file doesn't exist
    #     print(f"Dataset {dataset_id} not found, generating random data")
    #     n_samples = 1000
    #     n_features = 10
    #
    #     X = np.random.rand(n_samples, n_features)
    #     # Generate a classification or regression target based on first feature
    #     y = (X[:, 0] > 0.5).astype(int)  # Classification
    #     # Or: y = X[:, 0] * 10 + 5 + np.random.randn(n_samples)  # Regression
    #
    #     return X, y

    X_columns = train_param['feature_columns']
    y = train_param['target_column']

    # Load from file
    logger.info(f'Loading dataset {dataset_path}')
    df = pd.read_csv(dataset_path)
    # Assuming the last column is the target
    X = df[X_columns]
    y = df[y]

    return X, y


def get_model_instance(algorithm_name, parameters):
    """
    Dynamically create a model instance based on its name and parameters.

    Args:
        algorithm_name (str): Name of the algorithm class
        parameters (dict): Parameters for the model

    Returns:
        object: Instantiated model
    """
    # Check if we have the import statement for this model
    if algorithm_name in MODEL_IMPORTS:
        # Dynamically import the class
        exec(MODEL_IMPORTS[algorithm_name])
        # Create an instance with parameters
        model = eval(f"{algorithm_name}(**{parameters})")
        return model
    else:
        raise ValueError(f"Unsupported model: {algorithm_name}")


def check_model_type(model):

    if model is None:
        return "Model not found"

    if is_classifier(model):
        return "classifier"
    elif is_regressor(model):
        return "regressor"
    else:
        return "Unknown model type"


# def preprocess_data(redis_client, task):
#     """
#     Preprocess data according to the task specification.
#
#     Args:
#         redis_client: Redis client connection
#         task (dict): Task configuration
#
#     Returns:
#         dict: Preprocessing results
#     """
#     # Extract task parameters
#     algorithm_name = task.get('algorithm_name')
#     parameters = task.get('parameters', {})
#     dataset_id = task.get('dataset_id')
#
#     # Load the dataset
#     X, y = load_dataset(dataset_id, )
#
#     # Create transformer instance
#     transformer = get_model_instance(algorithm_name, parameters)
#
#     # Apply transformation
#     start_time = time.time()
#     X_transformed = transformer.fit_transform(X)
#     processing_time = time.time() - start_time
#
#     # Save the transformed data for subsequent tasks
#     transformed_dataset_id = f"{task.get('subtask_id')}_transformed"
#
#     # In a real implementation, save to a data store
#     # For this example, we'll save summary statistics
#     results = {
#         'transformer_type': algorithm_name,
#         'input_shape': X.shape,
#         'output_shape': X_transformed.shape,
#         'processing_time': processing_time,
#         'transformed_dataset_id': transformed_dataset_id
#     }
#
#     return results


if __name__ == "__main__":
    main()
