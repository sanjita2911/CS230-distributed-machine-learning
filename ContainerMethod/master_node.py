import logging
from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
import grpc
import time
import ml_task_pb2
import ml_task_pb2_grpc
import random
import os
import uuid
import threading
import requests
import kagglehub
import shutil
# from datasets import load_dataset  # Hugging Face datasets
import redis
import uuid

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # This sets the logging level to DEBUG
logger = logging.getLogger(__name__)

redis_client = redis.StrictRedis(host='redis-master', port=6379, db=0, decode_responses=True)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Worker tracking (initial execution times for workers)
worker_last_execution_time = {
    "worker1:50051": 0,
    "worker2:50051": 0,
    "worker3:50051": 0,
    "worker4:50051": 0,
}

workers = [
    "worker1:50051",
    "worker2:50051",
    "worker3:50051",
    "worker4:50051"
]

DATASET_PATH = "/mnt/efs/datasets/"     # EFS mounted directory


def select_worker():
    """
    Selects a worker for the task using a load balancing strategy.
    Here, we use a round-robin or least-recently-executed task approach.
    """
    # You can implement more sophisticated load balancing strategies here.
    # For now, let's pick the worker with the least execution time.
    least_recent_worker = min(worker_last_execution_time, key=worker_last_execution_time.get)
    return least_recent_worker


def send_task(worker, task_request):
    """
    Sends a task to the selected worker using gRPC and returns the result.
    """
    try:
        # Set up gRPC channel to the worker node
        channel = grpc.insecure_channel(worker)

        # Create a stub for the worker's service
        stub = ml_task_pb2_grpc.MLTaskServiceStub(channel)

        # Make the gRPC call to execute the task
        logger.info(f"Sending task to worker {worker}...")
        response = stub.ExecuteTask(task_request)

        logger.info(f"Task execution result from worker {worker}: {response.result}")

        # Return the result received from the worker
        return response

    except grpc.RpcError as e:
        logger.error(f"gRPC error when sending task to {worker}: {e.code()} - {e.details()}")
        raise e  # Re-raise the error to be caught in the Flask route

@app.route("/create_session", methods=["POST"])
def create_session():
    """Creates a new user session and stores it in Redis."""
    session_id = str(uuid.uuid4())
    redis_client.sadd("active_sessions", session_id)  # Add to active sessions
    redis_client.set(f"session:{session_id}:tasks_pending", 0)  # Initialize task counter
    return jsonify({"message": "Session created", "session_id": session_id}), 201


@app.route("/status/<session_id>", methods=["GET"])
def check_status(session_id):
    """Returns the number of pending tasks for a session."""
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404

    pending_tasks = int(redis_client.get(f"session:{session_id}:tasks_pending") or 0)
    return jsonify({"session_id": session_id, "tasks_pending": pending_tasks})

@app.route("/execute_task", methods=["POST"])
def execute_task():
    try:
        logger.info("Received request at /execute_task")

        # Log request headers and JSON body
        logger.debug("Request Headers: %s", request.headers)
        logger.debug("Request JSON: %s", request.get_json())

        # Check if the request contains JSON data
        data = request.get_json(force=True)

        if not data:
            logger.warning("No JSON body received.")
            return jsonify({"error": "No JSON body received"}), 400

        # Extract details from the request
        session_id = data.get("session_id")
        algorithm = data.get("algorithm")
        dataset_id = data.get("dataset_id")
        hyperparameters = data.get("hyperparameters")

        if not algorithm or not dataset_id or not hyperparameters:
            logger.warning("Missing required parameters.")
            return jsonify({"error": "Missing required parameters"}), 400

        if not redis_client.sismember("active_sessions", session_id):
            return jsonify({"error": "Invalid session ID"}), 404

        logger.info("Algorithm: %s, Dataset: %s, Hyperparameters: %s", algorithm, dataset_id, hyperparameters)

        # Select a worker dynamically using load balancing
        worker = select_worker()
        logger.info(f"Selected worker: {worker}")

        # Create a gRPC TaskRequest message
        task_request = ml_task_pb2.TaskRequest(
            algorithm=algorithm,
            dataset_id=dataset_id,
            hyperparameters=hyperparameters
        )

        redis_client.incr(f"session:{session_id}:tasks_pending")

        # Send task to the selected worker node
        start_time = time.time()
        response = send_task(worker, task_request)
        end_time = time.time()

        # Update the last execution time for the selected worker
        worker_last_execution_time[worker] = end_time - start_time
        logger.info("Task execution completed in %.2f seconds.", end_time - start_time)
        redis_client.decr(f"session:{session_id}:tasks_pending")

        return jsonify({
            "worker_id": response.worker_id,
            "result": response.result,
            "execution_time": response.execution_time
        }), 200

    except KeyError as e:
        logger.error("KeyError: Missing key %s", str(e))
        return jsonify({"error": f"Missing key: {str(e)}"}), 400

    except grpc.RpcError as e:
        logger.error("gRPC error: %s - %s", e.code(), e.details())
        return jsonify({"error": f"gRPC error: {e.code()} - {e.details()}"}), 500

    except Exception as e:
        logger.error("Internal server error: %s", str(e))
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500


@app.route('/create_session', methods=['POST'])
def create_session():
    session_id = str(uuid.uuid4())
    return jsonify({"session_id": session_id})


@app.route('/check_data', methods=['GET'])
def check_data_exists():
    dataset_name = request.args.get("dataset_name")
    dataset_path = os.path.join(DATASET_PATH, f"{dataset_name}")

    if os.path.exists(dataset_path):
        return jsonify({"exists": True, "path": dataset_path})
    return jsonify({"exists": False, "message": "Dataset not found"})


@app.route('/download_data', methods=['POST'])
def download_data():
    data = request.get_json()
    url = data.get("dataset_url")
    name = data.get("dataset_name")
    type = data.get("dataset_type")

    if not url or not name or not type:
        return jsonify({"error": "Missing dataset parameters"}), 400

    dataset_path = os.path.join(DATASET_PATH, f"{name}")
    os.makedirs(dataset_path, exist_ok=True)

    # Run the download in a separate thread to avoid blocking API
    thread = threading.Thread(target=download_dataset, args=(url, type, dataset_path))
    thread.start()

    return jsonify({"message": "Dataset download started", "dataset_path": dataset_path})


def download_dataset(url, type, dataset_path):
    if type == "kaggle":
        temp_path = kagglehub.dataset_download(url)      # download from kaggle

        shutil.move(temp_path, dataset_path)        # upload to efs

    # Download from Hugging Face
    # elif "huggingface.co" in dataset_url:
    #     dataset_name = dataset_url.split("/")[-1]
    #     dataset = load_dataset(dataset_name)
    #     dataset.save_to_disk(dataset_path)

    # Download from Direct URL
    # else:
    #     response = requests.get(dataset_url, stream=True)
    #     filename = dataset_url.split("/")[-1]
    #     file_path = os.path.join(dataset_path, filename)

    #     with open(file_path, "wb") as file:
    #         for chunk in response.iter_content(chunk_size=1024):
    #             if chunk:
    #                 file.write(chunk)

    print(f"Dataset downloaded successfully to {dataset_path}")


@app.route('/check_status', methods=['GET'])
def check_status():
    return None


@app.route('/balance_load', methods=['POST'])
def balance_load():
    return None


@app.route('/distribute_task', methods=['POST'])
def distribute_task():
    return None

    # input: model, params list, dataset_name
    # return session_id, dataset_name, model, params combos


if __name__ == "__main__":
    logger.info("Starting Flask server...")
    app.run(host="0.0.0.0", port=5001)
