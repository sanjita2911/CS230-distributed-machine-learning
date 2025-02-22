import requests
import time

API_URL = "https://your-api.com/train"
STATUS_URL = "https://your-api.com/status"


def DisGridSearch(data):
    """
    Sends data to a backend API and polls for real-time status updates.

    Args:
        data (dict): The data to send for training.

    Returns:
        dict: The final response from the backend.
    """
    response = requests.post(API_URL, json=data)
    if response.status_code != 200:
        raise Exception(f"Failed to start training: {response.text}")

    job_id = response.json()

    while True:
        status_response = requests.get(f"{STATUS_URL}/{job_id}")
        if status_response.status_code != 200:
            raise Exception("Failed to fetch status.")

        status = status_response.json()
        print(f"Training Status: {status['status']}")

        if status["status"] in ["COMPLETED", "FAILED"]:
            return status

        time.sleep(5)  # Poll every 5 seconds
