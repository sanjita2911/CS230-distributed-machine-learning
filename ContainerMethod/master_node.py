import logging
from flask import Flask, request, jsonify
from flask_cors import CORS  # Import CORS
import grpc
import time
import ml_task_pb2
import ml_task_pb2_grpc
import random

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # This sets the logging level to DEBUG
logger = logging.getLogger(__name__)

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
        algorithm = data.get("algorithm")
        dataset_id = data.get("dataset_id")
        hyperparameters = data.get("hyperparameters")

        if not algorithm or not dataset_id or not hyperparameters:
            logger.warning("Missing required parameters.")
            return jsonify({"error": "Missing required parameters"}), 400

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

        # Send task to the selected worker node
        start_time = time.time()
        response = send_task(worker, task_request)
        end_time = time.time()

        # Update the last execution time for the selected worker
        worker_last_execution_time[worker] = end_time - start_time
        logger.info("Task execution completed in %.2f seconds.", end_time - start_time)

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


if __name__ == "__main__":
    logger.info("Starting Flask server...")
    app.run(host="0.0.0.0", port=5000)
