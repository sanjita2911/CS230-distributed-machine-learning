import requests
import time
import uuid
from tqdm import tqdm

CREATE_SESSION_URL = "https://your-api.com/create_session"
CHECK_DATA_URL = "https://your-api.com/check-data"
DOWNLOAD_DATA_URL = "https://your-api.com/download-data"
GRIDSEARCH_URL = "https://your-api.com/gridsearch"
CHECK_STATUS_URL = "https://your-api.com/check-status"


def distributedMLSession():
    """
    Creates a unique session ID for the user.

    Returns:
        str: The generated session ID.
    """
    session_id = str(uuid.uuid4())  # Generate unique session ID
    response = requests.post(CREATE_SESSION_URL, json={"session_id": session_id})

    if response.status_code != 200:
        raise Exception(f"Failed to create session: {response.text}")

    print(f"Session Created: {session_id}")
    return session_id


def checkData(session_id, data_link, data_name):
    """
    Calls API to check the availability of data.

    Args:
        session_id (str): Unique session ID.
        data_link (str): URL where the dataset is hosted.
        data_name (str): Name of the dataset.

    Returns:
        dict: API response.
    """
    payload = {
        "session_id": session_id,
        "data_link": data_link,
        "data_name": data_name
    }
    response = requests.get(CHECK_DATA_URL, params=payload)

    if response.status_code != 200:
        raise Exception(f"Failed to check data: {response.text}")

    return response.json()


def downloadData(session_id, data_link, data_name):
    """
    Calls API to download data.

    Args:
        session_id (str): Unique session ID.
        data_link (str): URL where the dataset is hosted.
        data_name (str): Name of the dataset.
    Returns:
        dict: API response.
    """
    payload = {
        "session_id": session_id,
        "data_link": data_link,
        "data_name": data_name
    }
    response = requests.post(DOWNLOAD_DATA_URL, json=payload)

    if response.status_code != 200:
        raise Exception(f"Failed to download data: {response.text}")

    return response.json()


def gridSearchCV(session_id, data_name, estimator, param_grid):
    """
    Calls API to perform grid search with given parameters.

    Args:
        session_id (str): Unique session ID.
        data_name (str): Name of the dataset.
        param_grid (dict): Dictionary of hyperparameters to search.

    Returns:
        dict: API response.
    """
    payload = {
        "session_id": session_id,
        "data_name": data_name,
        "estimator": estimator,
        "param_grid": param_grid
    }

    response = requests.post(GRIDSEARCH_URL, json=payload)

    if response.status_code != 200:
        raise Exception(f"Failed to start grid search: {response.text}")

    job_id = response.json().get("job_id")

    # Polling with tqdm progress bar
    with tqdm(total=100, desc="Grid Search Progress") as pbar:
        while True:
            status = checkStatus(session_id)
            progress = status.get("progress", 0)  # Assuming API returns progress percentage

            pbar.update(progress - pbar.n)  # Update progress bar

            if status["status"] in ["COMPLETED", "FAILED"]:
                break

            time.sleep(5)  # Poll every 5 seconds

    return status


def checkStatus(session_id):
    """
    Calls API to check job status.

    Args:
        session_id (str): Unique session ID.

    Returns:
        dict: API response containing job status.
    """
    payload = {"session_id": session_id}
    response = requests.get(CHECK_STATUS_URL, params=payload)

    if response.status_code != 200:
        raise Exception(f"Failed to check status: {response.text}")

    return response.json()