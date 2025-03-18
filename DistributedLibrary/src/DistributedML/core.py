import requests
import json
import time
import uuid
import numpy as np
import pandas as pd
import math
from tqdm import tqdm
from sklearn.base import BaseEstimator
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV

class MLTaskManager:
    def __init__(self):
        """
        Initializes the task manager with the API base URL.
        
        Args:
            api_url (str): Base URL of the API endpoint
        """
        self.api_url = "http://127.0.0.1:5002"
        self.session_id = str(uuid.uuid4())  # Unique session ID
        self._create_session()
    
    def _create_session(self):
        """Creates a new session with the API."""
        response = requests.post(f"{self.api_url}/create_session", json={"session_id": self.session_id})
        if response.status_code != 200:
            raise Exception(f"Failed to create session: {response.text}")
        print(f"Session Created: {self.session_id}")
    
    def _api_request(self, endpoint, method="post", data=None, params=None):
        """
        Handles API requests with error handling and serialization.
        """
        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        headers = {"Content-Type": "application/json"}

        try:
            if data:
                data = json.loads(json.dumps(data, default=self._json_serializer))
                data = self._clean_dict(data)
            
            response = requests.request(method, url, json=data, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"status": "error", "message": str(e)}
    
    def _json_serializer(self, obj):
        """Serializes numpy and pandas objects into JSON-compatible formats."""
        if isinstance(obj, (np.float32, np.float64)):
            return None if math.isnan(obj) or math.isinf(obj) else float(obj)
        elif isinstance(obj, (np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (pd.DataFrame, pd.Series)):
            return obj.to_dict()
        return str(obj)
    
    def _clean_dict(self, data):
        """Recursively removes NaN and Inf values from dictionaries."""
        if isinstance(data, dict):
            return {k: self._clean_dict(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_dict(v) for v in data]
        elif isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
            return None
        return data
    
    def check_data(self, data_name):
        """Checks the availability of the data."""
        return self._api_request("check-data", "get", params={
            "session_id": self.session_id,
            "data_name": data_name
        })
    
    def download_data(self, data_link, data_name, data_type):
        """Downloads data from the provided link."""
        return self._api_request("download-data", "post", data={
            "session_id": self.session_id,
            "data_link": data_link,
            "data_name": data_name,
            "data_type": data_type
        })
    
    def _extract_model_details(self, estimator):
        """Extracts model type and hyperparameters."""
        if isinstance(estimator, (GridSearchCV, RandomizedSearchCV)):
            base_estimator = estimator.estimator
            return {
                'model_type': type(base_estimator).__name__,
                'search_type': "GridSearchCV" if isinstance(estimator, GridSearchCV) else "RandomizedSearchCV",
                'hyperparameters': {
                    'base_estimator_params': base_estimator.get_params(),
                    'search_params': estimator.param_grid if isinstance(estimator, GridSearchCV) else estimator.param_distributions
                }
            }
        return {'model_type': type(estimator).__name__, 'hyperparameters': estimator.get_params()}
    
    def create_training_job(self, estimator, dataset_name, train_params=None, wait_for_completion=False):
        """Submits a training job to the API."""
        job_id = str(uuid.uuid4())
        train_params = train_params or {'test_size': 0.2}
        payload = {
            'job_id': job_id,
            'session_id': self.session_id,
            'dataset_name': dataset_name,
            'model_details': self._extract_model_details(estimator),
            'train_params': train_params
        }
        api_response = self._api_request("train", "post", data=payload)
        if wait_for_completion and api_response.get("status") != "error":
            return self._wait_for_completion(job_id)
        return api_response
    
    def check_job_status(self, job_id):
        """Checks the status of a training job."""
        return self._api_request("check-status", "get", params={"session_id": self.session_id, "job_id": job_id})
    
    def _wait_for_completion(self, job_id, polling_interval=5, timeout=3600):
        """Waits for a training job to complete, displaying progress."""
        start_time = time.time()
        with tqdm(total=100, desc="Training Progress") as pbar:
            while True:
                if time.time() - start_time > timeout:
                    return {"status": "error", "message": "Timeout exceeded"}
                status = self.check_job_status(job_id)
                progress = status.get("progress", 0)
                pbar.update(progress - pbar.n)
                if status.get("status") in ["COMPLETED", "FAILED"]:
                    return status
                time.sleep(polling_interval)
    
    def grid_search_cv(self, estimator, dataset_name, param_grid):
        """Performs Grid Search CV on the dataset."""
        job_id = str(uuid.uuid4())
        payload = {
            "session_id": self.session_id,
            "dataset_name": dataset_name,
            "estimator": type(estimator).__name__,
            "param_grid": param_grid
        }
        response = self._api_request("gridsearch", "post", data=payload)
        if response.get("status") == "error":
            return response
        with tqdm(total=100, desc="Grid Search Progress") as pbar:
            while True:
                status = self.check_job_status(job_id)
                progress = status.get("progress", 0)
                pbar.update(progress - pbar.n)
                if status.get("status") in ["COMPLETED", "FAILED"]:
                    return status
                time.sleep(5)


if __name__ == "__main__":
    task_manager = MLTaskManager()

    # download & check data
    task_manager.download_data("uciml/iris", "iris", "kaggle")
    data_status = task_manager.check_data("iris")
    print(data_status)

    # create single training job
    from sklearn.ensemble import RandomForestClassifier
    rf = RandomForestClassifier(n_estimators=100, max_depth=5)
    job_response = task_manager.create_training_job(
        rf, 
        dataset_name="iris",
        train_params={
            'test_size': 0.25,
            'random_state': 42,
            'feature_columns': ['sepal_length','sepal_width','petal_length','petal_width'],
            'target_column': 'species'
        },
        wait_for_completion=True
    )

    # create gridsearch job
    from sklearn.linear_model import LogisticRegression
    param_grid = {
        'C': [0.1, 1.0, 10.0, 100],
        'solver': ['liblinear', 'lbfgs']
    }
    lr = LogisticRegression()
    
    job_response = task_manager.grid_search_cv(
        lr, 
        dataset_name="iris",
        param_grid=param_grid
    )

