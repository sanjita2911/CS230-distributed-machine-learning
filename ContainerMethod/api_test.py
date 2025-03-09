import requests
# from DistributedML import DisGridSearch

# url = "http://localhost:5001/execute_task"
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

import requests
import time
import json

# Base URL of the Flask API
base_url = "http://127.0.0.1:5001"

# Function to create a session
def create_session():
    response = requests.post(f"{base_url}/create_session")
    if response.status_code == 201:
        session_id = response.json().get("session_id")
        print(f"Session created with ID: {session_id}")
        return session_id
    else:
        print("Failed to create session")
        return None

# Function to execute a task
def execute_task(session_id, task_payload):
    """Execute a task by sending a POST request."""
    task_payload["session_id"] = session_id
    response = requests.post(f"{base_url}/execute_task", json=task_payload)
    if response.status_code == 200:
        print(f"Task executed: {response.json()}")
    else:
        print(f"Failed to execute task: {response.json()}")
# Function to check task status
def check_status(session_id):
    response = requests.get(f"{base_url}/status/{session_id}")
    if response.status_code == 200:
        print(f"Status for session {session_id}: {response.json()}")
    else:
        print(f"Failed to get status for session {session_id}: {response.json()}")


def main():
    # Step 1: Create a session
    session_id = create_session()
    if not session_id:
        return

    # Step 2: Send 6 tasks to the Flask API
    for task in payload:
        execute_task(session_id, task)
        check_status(session_id)
        time.sleep(5)  # Add a short delay between requests
        check_status(session_id)

    # Step 3: Check the task status after submitting all tasks
    time.sleep(2)  # Wait for the tasks to be processed
    check_status(session_id)

if __name__ == "__main__":
    main()
