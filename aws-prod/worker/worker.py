import json
import os
import pickle
import time
import uuid
import redis
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka_util import create_kafka_consumer,KafkaProducerSingleton
from config import KAFKA_TRAIN_TOPIC,KAFKA_RESULTS_TOPIC,REDIS_ADDRESS
from sklearn.base import BaseEstimator
from sklearn.pipeline import Pipeline
from logger_util import logger
from sklearn.base import is_classifier, is_regressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, r2_score, mean_squared_error
from redis_util import create_redis_client

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
redis_client = create_redis_client()

def main():
    """
    Main worker function that consumes ML training tasks from Kafka,
    processes them, and sends results back.
    """
    # Initialize Redis connection
    # redis_client = redis.Redis(host=REDIS_ADDRESS, port=6379, db=0)
    # Initialize Kafka consumer
    consumer = create_kafka_consumer(KAFKA_TRAIN_TOPIC)
    # Initialize Kafka producer for results
    producer = KafkaProducerSingleton.get_producer()
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

            # # Process the task based on its type
            result = process_task(task)
            #
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
    """
    Train a model according to the task specification.

    Args:
        redis_client: Redis client connection
        task (dict): Task configuration

    Returns:
        dict: Training results
    """
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
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

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
    dataset_path = f"/mnt/efs/datasets/{dataset_id}/{dataset_id}.csv"
    # dataset_path = f"/mnt/datasets/{dataset_id}.csv" # ON AWS uncomment

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