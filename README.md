# distributed-ml

distributed-ml is a Python-based machine learning package that facilitates model training, hyperparameter tuning, and data handling through a structured API. It supports various scikit-learn estimators, including classifiers, regressors, and hyperparameter search classes like GridSearchCV and RandomizedSearchCV.

## Features
- Create and manage API sessions
- Check and download datasets
- Train models with various configurations
- Monitor job status with progress bars
- Handle hyperparameter tuning efficiently

## Installation

To install `distributed-ml`, run:

```sh
pip install distributed-ml
```

## Importing the Package

```python
from distributed_ml import MLTaskManager
```

## API Reference

### `MLTaskManager()`
- Instantiates the class and returns a session ID.

```python
manager = MLTaskManager()
```

### `check_data(data_name)`
- Checks if a dataset is available.
- **Arguments:**
    - `data_name` (str): Name of the dataset.
- **Returns:** Path where the data was downloaded, or a `404 Error` if unavailable.

```python
manager.check_data("iris")
```

### `download_data(data_link, data_name, data_type)`
- Downloads data from a specified source.
- **Arguments:**
    - `data_link` (str): URL or dataset identifier.
    - `data_name` (str): Name to save the dataset as.
    - `data_type` (str): Source type (e.g., "kaggle").
- **Returns:** Path where the data was downloaded.

```python
manager.download_data("himanshunakrani/iris-dataset", "iris", "kaggle")
```

### `train(estimator, dataset_name, train_params=None, wait_for_completion=False)`
- Submits a training job to the API.
- **Arguments:**
    - `estimator`: A scikit-learn model.
    - `dataset_name` (str): Name of the dataset.
    - `train_params` (dict, optional): Training configurations.
    - `wait_for_completion` (bool, optional): Whether to wait for the job to complete.
- **Returns:** Training progress and job results (i.e., best results, best parameters).

```python
from sklearn.ensemble import RandomForestClassifier

rf = RandomForestClassifier(n_estimators=100, max_depth=5)
job_response = manager.train(
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
print(job_response.get('job_result'))
```

### Hyperparameter Tuning Examples

#### GridSearchCV

```python
from sklearn.model_selection import GridSearchCV
from sklearn.linear_model import LogisticRegression

param_grid = {
    'C': [0.1, 1.0, 10.0, 100],
    'solver': ['liblinear', 'lbfgs']
}

lr = LogisticRegression()
grid_search = GridSearchCV(lr, param_grid, cv=5)

job_response = manager.train(
    grid_search, 
    dataset_name="iris",
    train_params={
        'test_size': 0.25,
        'random_state': 42,
        'feature_columns': ['sepal_length','sepal_width','petal_length','petal_width'],
        'target_column': 'species'
    },
    wait_for_completion=True
)
print(job_response.get('best_result'))
```

#### RandomizedSearchCV

```python
from sklearn.model_selection import RandomizedSearchCV
from sklearn.linear_model import LogisticRegression

random_search = RandomizedSearchCV(lr, param_grid, cv=5)

job_response = manager.train(
    random_search, 
    dataset_name="iris",
    train_params={
        'test_size': 0.25,
        'random_state': 42,
        'feature_columns': ['sepal_length','sepal_width','petal_length','petal_width'],
        'target_column': 'species'
    },
    wait_for_completion=True
)
print(job_response.get('best_result'))
```

### `check_job_status(job_id)`
- Retrieves the status of a training job.
- **Arguments:**
    - `job_id` (str): Unique job identifier.
- **Returns:** Training progress and job results (i.e., best results, best parameters).

```python
manager.check_job_status("job_12345")
```

## License
This project is licensed under the MIT License.

