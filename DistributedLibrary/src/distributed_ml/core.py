import requests
import json
from datetime import datetime
import uuid
import numpy as np
from tqdm import tqdm
import numpy as np
import time
import math
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV


class MLTaskManager:
    def __init__(self, url):
        """
        Initializes the task manager with the API base URL.

        Args:
            api_url (str): Base URL of the API endpoint
        """
        self.api_url = url
        self.session_id = self._create_session()  # Unique session ID
        self.job_id = None
        self.result = None

    def _create_session(self):
        """Creates a new session with the API."""
        response = requests.post(f"{self.api_url}/create_session")
        if response.status_code == 200 or 201:
            session_id = response.json().get("session_id")
            print(f"Session Created: {session_id}")
            return session_id
        else:
            raise Exception(f"Failed to create session: {response.text}")

    def _api_request(self, endpoint, method="post", data=None, params=None):
        """
        Handles API requests with error handling and serialization.
        """
        if not self.api_url:
            print("Warning: API URL not provided.")
            return {"status": "error", "message": "API URL not provided"}

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
        return self._api_request(f"check_data/{self.session_id}", "get", params={
            "dataset_name": data_name
        })

    def download_data(self, data_link, data_name, data_type):
        """Downloads data from the provided link."""
        return self._api_request(f"download_data/{self.session_id}", "post", data={
            "dataset_url": data_link,
            "dataset_name": data_name,
            "dataset_type": data_type
        })

    def _extract_model_details(self, estimator):
        """
        Extract model type and hyperparameters from a scikit-learn estimator.

        Args:
            estimator: A scikit-learn estimator, GridSearchCV, or RandomizedSearchCV object

        Returns:
            dict: Dictionary containing model type and hyperparameters
        """
        if isinstance(estimator, (GridSearchCV, RandomizedSearchCV)):
            base_estimator = estimator.estimator
            model_type = type(base_estimator).__name__
            search_type = "GridSearchCV" if isinstance(estimator, GridSearchCV) else "RandomizedSearchCV"

            # Get param distributions/grid
            if isinstance(estimator, GridSearchCV):
                param_search = {
                    'param_grid': estimator.param_grid
                }
            else:  # RandomizedSearchCV
                param_search = {
                    'param_distributions': estimator.param_distributions,
                    'n_iter': estimator.n_iter
                }

            # Get CV parameters
            cv_params = {
                'cv': estimator.cv,
                'scoring': estimator.scoring,
                'refit': estimator.refit,
                'verbose': estimator.verbose,
                'error_score': estimator.error_score,
                'return_train_score': estimator.return_train_score
            }

            hyperparams = {
                'base_estimator_params': {k.split('__')[-1]: v for k, v in base_estimator.get_params().items()},
                'search_params': param_search,
                'cv_params': cv_params
            }

            return {
                'model_type': model_type,
                'search_type': search_type,
                'hyperparameters': hyperparams
            }
        else:
            model_type = type(estimator).__name__
            hyperparams = {k: v for k, v in estimator.get_params().items()}

            return {
                'model_type': model_type,
                'hyperparameters': hyperparams
            }

    def train(self, estimator, dataset_name, train_params=None, wait_for_completion=False):
        """Submits a training job to the API."""
        self.job_id = str(uuid.uuid4())

        if train_params is None:
            train_params = {}
        if 'test_size' not in train_params and not isinstance(estimator, (GridSearchCV, RandomizedSearchCV)):
            train_params['test_size'] = 0.2

        payload = {
            'job_id': self.job_id,
            'session_id': self.session_id,
            'dataset_id': dataset_name,
            'model_details': self._extract_model_details(estimator),
            'train_params': train_params,
            'timestamp': datetime.now().isoformat()
        }
        api_response = self._api_request(f"train_status/{self.session_id}", "post", data=payload)
        print("Job Created:", self.job_id)
        print(api_response['status'])
        if wait_for_completion and api_response.get("status") != "error":
            return self._wait_for_completion(self.job_id)
        return api_response

    def check_job_status(self, job_id):
        """Checks the status of a training job."""
        return self._api_request(f"metrics/{self.session_id}/{job_id}", "get")

    def _wait_for_completion(self, job_id, polling_interval=1, timeout=60):
        """Waits for a training job to complete, displaying progress."""
        start_time = time.time()
        with tqdm(total=100, desc="Training Progress") as pbar:
            while True:
                if time.time() - start_time > timeout:
                    return {"status": "error", "message": "Timeout exceeded"}
                status = self.check_job_status(job_id)
                progress = status.get("job_status", 0)
                if progress == "pending":
                    value = 0
                elif progress == "completed":
                    value = 100
                else:
                    value = int(float(progress))
                pbar.update(value - pbar.n)
                if status.get("job_status") in ["completed", "failed"]:
                    self.result = status
                    return status
                time.sleep(polling_interval)

    def download_best_model(self, job_id, model_path, model_id):
        """Downloads the best result from training job."""
        return self._api_request(f"/download_model/{self.session_id}/{job_id}", "post", data={
            "model_path": model_path,
            "model_id": model_id
        })

    def preprocess(self, dataset_name, yaml):
        """Submits a preprocessing job to the API."""
        return self._api_request(f"preprocess/{self.session_id}", "post", data={
            "dataset_id": dataset_name,
            "yaml_url": yaml
        })