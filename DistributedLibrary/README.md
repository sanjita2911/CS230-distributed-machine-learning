# distributed-ml

distributed-ml is a Python-based machine learning package that facilitates model training, hyperparameter tuning, and data handling through a structured API. It supports various scikit-learn estimators, including classifiers, regressors, and hyperparameter search classes like GridSearchCV and RandomizedSearchCV.

This package allows users to:
- Create and manage API sessions
- Check and download datasets
- Train models with various configurations
- Monitor job status with progress bars
- Handle hyperparameter tuning efficiently

## API Reference

#### MLTaskManager()
- Instantiates class.
- Returns session id.

#### check_data(data_name)
- Checks if a dataset is available.
- Arguments:
    - data_name (str): Name of the dataset.
- Returns path where data was downloaded, otherwise 404 Error.

#### download_data(data_link, data_name, data_type)
- Downloads data from a specified source.
- Arguments:
    - data_link (str): URL or dataset identifier.
    - data_name (str): Name to save dataset as.
    - data_type (str): Source type (e.g., “kaggle”).
- Returns path where data was downloaded.

#### preprocess(self, dataset_name, yaml)
- Submits a preprocessing job to the API.
- Arguments:
    - dataset_name (str): Name of dataset.
    - yaml (str): Path to yaml configuration file.

#### train(estimator, dataset_name, train_params=None, wait_for_completion=False)
- Submits a training job to the API.
- Arguments:
    - estimator: A scikit-learn model.
    - dataset_name (str): Name of dataset.
    - train_params (dict, optional): Training configurations.
    - wait_for_completion (bool, optional): Whether to wait for the job to complete.
- Returns training progress and job results (i.e. best results, best parameters)

#### check_job_status(job_id)
- Retrieves the status of a training job.
- Arguments:
    - job_id (str): Unique job identifier.
- Returns training progress and job results (i.e. best results, best parameters)

#### download_best_model(self, job_id, model_path, model_id)
- Downloads best model from a job.
- Arguments:
    - job_id (str): Unique job identifier.
    - model_path (str): Path to store model.
    - model_id (str): Unique model identifier.