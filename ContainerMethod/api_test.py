import requests

url = "http://localhost:5001/execute_task"
payload = [{
    "algorithm": "RandomForest",
    "dataset_id": "dataset1",
    "hyperparameters": {"n_estimators": "100", "max_depth": "5"}
},{
    "algorithm": "RandomForest",
    "dataset_id": "dataset1",
    "hyperparameters": {"n_estimators": "100", "max_depth": "5"}
},{
    "algorithm": "RandomForest",
    "dataset_id": "dataset1",
    "hyperparameters": {"n_estimators": "100", "max_depth": "5"}
},{
    "algorithm": "RandomForest",
    "dataset_id": "dataset1",
    "hyperparameters": {"n_estimators": "100", "max_depth": "5"}
},{
    "algorithm": "RandomForest",
    "dataset_id": "dataset1",
    "hyperparameters": {"n_estimators": "100", "max_depth": "5"}
}]

payload1 = {
    "algorithm": "RandomForest",
    "dataset_id": "dataset1",
    "hyperparameters": {"n_estimators": "100", "max_depth": "5"}
}

# response = requests.post(url, json=payload1)

# Send the request
for i in payload:
    response = requests.post(url, json=i)

# Print status code and response
print(f"Status Code: {response.status_code}")
print("Response Text:", response.text)

# If the response is JSON, print it
if response.status_code == 200:
    print("Response JSON:", response.json())
else:
    print("Error response:", response.text)
