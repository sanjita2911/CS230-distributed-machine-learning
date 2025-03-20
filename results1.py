from distributed_ml import MLTaskManager
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
import kagglehub
import pandas as pd
from sklearn.model_selection import train_test_split
import time
import numpy as np
import matplotlib.pyplot as plt


task_manager = MLTaskManager()

# TODO: wanna try on this high-dim dataset instead, for now using iris

# Download the dataset
# download_status = task_manager.download_data("krishd123/high-dimensional-datascape", "high", "kaggle")
# print(download_status)

# # Verify the dataset
# data_status = task_manager.check_data("high")
# print(data_status)


# Download the dataset locally
path = kagglehub.dataset_download("himanshunakrani/iris-dataset")
iris = pd.read_csv(f"{path}/iris.csv")
X, y = iris[["sepal_length", "sepal_width", "petal_length", "petal_width"]], iris["species"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)


rf = RandomForestClassifier()
param_grid = {
    'n_estimators': [100, 200, 500],
    'max_depth': [10, 20, 50, None],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4],
    'bootstrap': [True, False]
}



# Distributed Training
job_response = task_manager.train(
    rf,
    dataset_name="iris",
    train_params={
        'test_size': 0.25,
        'random_state': 42,
        'feature_columns': ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
        'target_column': 'species'
    },
    wait_for_completion=True
)
train_time1 = 0
for result in job_response.get('job_result')['results']:
    train_time1 += result["training_time"]
print("Distributed total training time:", round(train_time1, 4), "seconds")

# Non-Distributed Training
start_time = time.time()
rf.fit(X_train, y_train)
train_time2 = time.time() - start_time
print("Non-Distributed total training time:", round(train_time2, 4), "seconds\n")




# Distributed GridSearchCV
grid_search = GridSearchCV(rf, param_grid, cv=5, n_jobs=-1)

job_response = task_manager.train(
    grid_search,
    dataset_name="iris",
    train_params={
        'test_size': 0.25,
        'random_state': 42,
        'feature_columns': ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
        'target_column': 'species'
    },
    wait_for_completion=True
)
grid_time1 = 0
for result in job_response.get('job_result')['results']:
    grid_time1 += result["training_time"]
print("Distributed total gridsearchcv time:", round(grid_time1, 4), "seconds")

# Non-Distributed GridSearchCV
start_time = time.time()
grid_search.fit(X_train, y_train)
grid_time2 = time.time() - start_time
print("Non-Distributed total gridsearchcv time:", round(grid_time2, 4), "seconds\n")



# RandomizedSearchCV
random_search = RandomizedSearchCV(rf, param_grid, n_iter=50, cv=5, n_jobs=-1, random_state=42)

job_response = task_manager.train(
    random_search,
    dataset_name="iris",
    train_params={
        'test_size': 0.25,
        'random_state': 42,
        'feature_columns': ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
        'target_column': 'species'
    },
    wait_for_completion=True
)
random_time1 = 0
for result in job_response.get('job_result')['results']:
    random_time1 += result["training_time"]
print("Distributed total randomizedsearchcv time:", round(random_time1, 4), "seconds")

# Non-Distributed GridSearchCV
start_time = time.time()
random_search.fit(X_train, y_train)
random_time2 = time.time() - start_time
print("Non-Distributed total randomizedsearchcv time:", round(random_time2, 4), "seconds\n")




# RESULTS GRAPH 1
methods = ["Training", "GridSearchCV", "RandomizedSearchCV"]

distributed_times = [train_time1, grid_time1, random_time1]
scikit_times = [train_time2, grid_time2, random_time2]

bar_width = 0.35
x = np.arange(len(methods))

fig, ax = plt.subplots(figsize=(8, 6))
ax.bar(x - bar_width/2, scikit_times, bar_width, label="scikit-learn", color='gray')
ax.bar(x + bar_width/2, distributed_times, bar_width, label="distributed-ml", color='blue')

ax.set_ylabel("Time (seconds)")
ax.set_title("Distributed vs. Non-Distributed Runtimes")
ax.set_xticks(x)
ax.set_xticklabels(methods)
ax.legend()

plt.show()