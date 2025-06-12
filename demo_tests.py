from distributed_ml import MLTaskManager
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier


# instantiate class
task_manager = MLTaskManager("http://127.0.0.1:5001")


# download & check data
download_status = task_manager.download_data("yasserh/titanic-dataset", "titanic", "kaggle")
print(download_status['message'], '\n')

data_status = task_manager.check_data("titanic")
print(data_status['status'], '\n')

# preprocessing job
job_response = task_manager.preprocess(
    dataset_name="titanic",
    yaml="titanic_preprocess.yaml"
)
print(job_response['message'])


# training job
rf = RandomForestClassifier(n_estimators=5)
job_response = task_manager.train(
    rf,
    dataset_name="titanic",
    train_params={
        'test_size': 0.25,
        'random_state': 42
    },
    wait_for_completion=False
)


# lr = LogisticRegression()
# # gridsearch job
# param_grid = {
#     'C': [0.1, 1.0, 10.0, 100],
#     'solver': ['liblinear', 'lbfgs']
# }
# grid_search = GridSearchCV(lr, param_grid, cv=5)

# job_response = task_manager.train(
#     grid_search,
#     dataset_name="titanic",
#     train_params={
#         'test_size': 0.25,
#         'random_state': 42,
#         'feature_columns': ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
#         'target_column': 'species'
#     },
#     wait_for_completion=True
# )




# # randomizedsearch job
# random_search = RandomizedSearchCV(lr, param_grid, cv=5)

# job_response = task_manager.train(
#     random_search,
#     dataset_name="titanic",
#     train_params={
#         'test_size': 0.25,
#         'random_state': 42,
#         'feature_columns': ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
#         'target_column': 'species'
#     },
#     wait_for_completion=True
# )