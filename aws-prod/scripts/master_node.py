# import os
# import time
# import uuid
# import grpc
# import logging
# import threading
# from flask import Flask, request, jsonify, session
# from flask_session import Session
# import redis
# from flask_cors import CORS

# import ml_task_pb2
# import ml_task_pb2_grpc

# logging.basicConfig(level=logging.INFO)
# app = Flask(__name__)
# CORS(app)

# # ---- Redis-based Flask Session ----
# app.secret_key = "mysecretkey"
# app.config["SESSION_TYPE"] = "redis"
# app.config["SESSION_REDIS"] = redis.Redis(
#     host=os.getenv("REDIS_HOST", "redis"),
#     port=int(os.getenv("REDIS_PORT", 6379))
# )
# Session(app)

# WORKERS = os.getenv("WORKERS", "worker1:50051,worker2:50051,worker3:50051,worker4:50051").split(",")
# worker_usage = {w: 0 for w in WORKERS}

# @app.before_request
# def assign_user_session():
#     # Each user gets a unique user_id
#     if "user_id" not in session:
#         session["user_id"] = str(uuid.uuid4())

# def create_stub(worker_address):
#     channel = grpc.insecure_channel(worker_address)
#     return ml_task_pb2_grpc.MLTaskServiceStub(channel)

# def download_dataset_on_worker(worker_address, dataset_id):
#     stub = create_stub(worker_address)
#     req = ml_task_pb2.DownloadRequest(dataset_id=dataset_id)
#     try:
#         resp = stub.DownloadDataset(req)
#         if resp.success:
#             app.logger.info(f"[{worker_address}] Download: {resp.message}")
#             return True
#         else:
#             app.logger.error(f"[{worker_address}] Download failed: {resp.message}")
#             return False
#     except Exception as e:
#         app.logger.exception(f"Error calling DownloadDataset on {worker_address}")
#         return False

# def execute_task_on_worker(worker_address, dataset_id, algorithm, hyperparams):
#     stub = create_stub(worker_address)
#     req = ml_task_pb2.TaskRequest(
#         dataset_id=dataset_id,
#         algorithm=algorithm,
#         hyperparameters=hyperparams
#     )

#     start_time = time.time()
#     try:
#         resp = stub.ExecuteTask(req)
#         worker_usage[worker_address] = time.time() - start_time
#         return {
#             "worker_id": resp.worker_id,
#             "result": resp.result,
#             "execution_time": resp.execution_time
#         }
#     except Exception as e:
#         app.logger.exception(f"Error calling ExecuteTask on {worker_address}")
#         return {
#             "worker_id": "unknown",
#             "result": str(e),
#             "execution_time": 0.0
#         }

# @app.route("/execute_training", methods=["POST"])
# def execute_training():
#     data = request.get_json(force=True)
#     if not data:
#         return jsonify({"error": "No JSON body"}), 400

#     dataset_id = data.get("dataset_id")
#     algorithm = data.get("algorithm")
#     hyper_list = data.get("hyperparameters_list")

#     if not dataset_id or not algorithm or not hyper_list:
#         return jsonify({"error": "Missing dataset_id, algorithm, or hyperparameters_list"}), 400

#     user_id = session.get("user_id", "unknown")
#     app.logger.info(f"User {user_id} is training {algorithm} on {dataset_id}")

#     # 1) Download dataset on all workers in parallel
#     download_threads = []
#     download_results = []

#     def worker_dl_thread(worker):
#         success = download_dataset_on_worker(worker, dataset_id)
#         download_results.append(success)

#     for w in WORKERS:
#         t = threading.Thread(target=worker_dl_thread, args=(w,))
#         t.start()
#         download_threads.append(t)
#     for t in download_threads:
#         t.join()

#     if not all(download_results):
#         return jsonify({"error": "At least one worker failed to download dataset"}), 500

#     # 2) Dispatch tasks to workers in a round-robin manner for every hyperparameter set
#     results = []
#     num_workers = len(WORKERS)
#     app.logger.info(f"Dispatching {len(hyper_list)} hyperparameter configurations in round-robin fashion")
#     for i, hyperparams in enumerate(hyper_list):
#         worker = WORKERS[i % num_workers]
#         app.logger.info(f"Dispatching hyperparams {hyperparams} to worker {worker}")
#         res = execute_task_on_worker(worker, dataset_id, algorithm, hyperparams)
#         # Attach the hyperparameters used for this task for later reference
#         res["hyperparameters"] = hyperparams
#         results.append(res)

#     app.logger.info("All worker results: " + str(results))

#     # 3) Determine the best result based on test accuracy
#     best_acc = -1.0
#     best_result = None
#     for r in results:
#         if "accuracy:" in r["result"]:
#             try:
#                 acc_str = r["result"].split("accuracy:")[1].strip()
#                 acc_val = float(acc_str)
#                 if acc_val > best_acc:
#                     best_acc = acc_val
#                     best_result = r
#             except Exception as e:
#                 app.logger.error(f"Error parsing accuracy: {e}")
#                 continue

#     if best_result is None:
#         return jsonify({"error": "No valid results returned from workers."}), 500

#     # 4) Return only the best result info to the user
#     final_response = {
#         "algorithm": algorithm,
#         "best_accuracy": best_acc,
#         "best_hyperparameters": best_result.get("hyperparameters", {})
#     }
#     return jsonify(final_response), 200


# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)

# import os
# import time
# import uuid
# import json
# import logging
# from flask import Flask, request, jsonify, session
# from flask_session import Session
# import redis
# from flask_cors import CORS
# from kafka import KafkaProducer, KafkaConsumer
# import logging

# logging.basicConfig(level=logging.INFO)
# app = Flask(__name__)
# CORS(app)

# # ---- Redis-based Flask Session ----
# app.secret_key = "mysecretkey"
# app.config["SESSION_TYPE"] = "redis"
# app.config["SESSION_REDIS"] = redis.Redis(
#     host=os.getenv("REDIS_HOST", "redis"),
#     port=int(os.getenv("REDIS_PORT", 6379))
# )
# Session(app)

# # Kafka settings
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# # Create Kafka Producer for sending task messages
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# # Create Kafka Consumer for receiving task responses
# consumer = KafkaConsumer(
#     "task_responses",
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#     auto_offset_reset="earliest",
#     group_id="master-group"
# )

# @app.before_request
# def assign_user_session():
#     if "user_id" not in session:
#         session["user_id"] = str(uuid.uuid4())

# @app.route("/execute_training", methods=["POST"])
# def execute_training():
#     data = request.get_json(force=True)
#     if not data:
#         return jsonify({"error": "No JSON body"}), 400

#     dataset_id = data.get("dataset_id")
#     algorithm = data.get("algorithm")
#     hyper_list = data.get("hyperparameters_list")
#     if not dataset_id or not algorithm or not hyper_list:
#         return jsonify({"error": "Missing dataset_id, algorithm, or hyperparameters_list"}), 400

#     user_id = session.get("user_id", "unknown")
#     request_id = str(uuid.uuid4())
#     app.logger.info(f"User {user_id} submitted training request {request_id} for {algorithm} on {dataset_id}")

#     # Publish each hyperparameter configuration as a separate task
#     num_tasks = len(hyper_list)
#     for i, hyperparams in enumerate(hyper_list):
#         assigned_worker = (i % 4) + 1  # optional, for debugging if you have 4 workers
#         task = {
#             "request_id": request_id,
#             "user_id": user_id,
#             "dataset_id": dataset_id,
#             "algorithm": algorithm,
#             "hyperparameters": hyperparams,
#             "assigned_worker": assigned_worker
#         }
#         producer.send("task_requests", task)
#         app.logger.info(f"Dispatched task {i+1} to worker {assigned_worker}: {hyperparams}")

#     producer.flush()

#     # Collect responses from Kafka for this request_id
#     results = []
#     timeout = time.time() + 30  # 30-second timeout
#     while time.time() < timeout and len(results) < num_tasks:
#         messages = consumer.poll(timeout_ms=500)
#         for msgs in messages.values():
#             for msg in msgs:
#                 result = msg.value
#                 # Check if the result belongs to this request_id
#                 if result.get("request_id") == request_id:
#                     results.append(result)
#                     app.logger.info(f"Received from Worker: {json.dumps(result, indent=2)}")    
#         if len(results) >= num_tasks:
#             break

#     if not results:
#         return jsonify({"error": "No results received from workers"}), 500

#     # Pick the best result (highest accuracy)
#     best_acc = -1.0
#     best_result = None
#     for r in results:
#         if "accuracy" in r:
#             try:
#                 acc_val = float(r["accuracy"])
#                 if acc_val > best_acc:
#                     best_acc = acc_val
#                     best_result = r
#             except Exception as e:
#                 app.logger.error(f"Error parsing accuracy: {e}")

#     if best_result is None:
#         return jsonify({"error": "No valid results returned from workers."}), 500

#     final_response = {
#         "algorithm": algorithm,
#         "best_accuracy": best_acc,
#         "best_hyperparameters": best_result.get("hyperparameters", {})
#     }
#     return jsonify(final_response), 200

# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)

# import os
# import time
# import json
# import uuid
# import logging
# from flask import Flask, request, jsonify, session
# from flask_cors import CORS
# from flask_session import Session
# from kafka import KafkaProducer, KafkaConsumer
# from kafka.errors import KafkaError
# import redis
# import threading

# # Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# # Flask app configuration
# app = Flask(__name__)
# CORS(app)

# # Redis and session configuration
# REDIS_HOST = os.getenv("REDIS_HOST", "redis")
# REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# app.config["SESSION_TYPE"] = "redis"
# app.config["SESSION_REDIS"] = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
# app.config["SESSION_PERMANENT"] = False
# app.config["SESSION_USE_SIGNER"] = True
# app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev_secret_key")
# Session(app)

# # Kafka configuration
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
# RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

# # Global variables
# task_results = {}
# task_locks = {}

# def create_kafka_producer(max_retries=3):
#     """Create a Kafka producer with retry logic"""
#     for attempt in range(max_retries):
#         try:
#             producer = KafkaProducer(
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#                 retries=3
#             )
#             return producer
#         except KafkaError as e:
#             if attempt < max_retries - 1:
#                 logger.warning(f"Failed to create Kafka producer (attempt {attempt+1}/{max_retries}): {e}")
#                 time.sleep(5)
#             else:
#                 logger.error(f"Failed to create Kafka producer after {max_retries} attempts: {e}")
#                 raise

# def create_kafka_consumer(max_retries=3):
#     """Create a Kafka consumer with retry logic"""
#     for attempt in range(max_retries):
#         try:
#             consumer = KafkaConsumer(
#                 "task_responses",
#                 bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#                 value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#                 auto_offset_reset="latest",
#                 group_id="master-group",
#                 session_timeout_ms=30000,
#                 heartbeat_interval_ms=10000
#             )
#             return consumer
#         except KafkaError as e:
#             if attempt < max_retries - 1:
#                 logger.warning(f"Failed to connect to Kafka (attempt {attempt+1}/{max_retries}): {e}")
#                 time.sleep(5)
#             else:
#                 logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
#                 raise

# def collect_results_thread(request_id, expected_count, timeout=300):
#     """
#     Thread function to collect results from workers.
    
#     Args:
#         request_id: The unique ID for this training request
#         expected_count: Number of results to expect
#         timeout: Maximum time to wait for results (in seconds)
#     """
#     try:
#         consumer = create_kafka_consumer()
        
#         # Initialize results for this request
#         task_results[request_id] = {
#             "completed": 0,
#             "expected": expected_count,
#             "results": [],
#             "status": "in_progress"
#         }
        
#         start_time = time.time()
#         results_count = 0
        
#         while results_count < expected_count and (time.time() - start_time) < timeout:
#             # Poll with a timeout of 1 second
#             messages = consumer.poll(timeout_ms=1000)
            
#             for topic_partition, records in messages.items():
#                 for record in records:
#                     result = record.value
                    
#                     # Check if this result belongs to our request
#                     if result.get("request_id") == request_id:
#                         with task_locks.get(request_id, threading.Lock()):
#                             task_results[request_id]["results"].append(result)
#                             results_count += 1
#                             task_results[request_id]["completed"] = results_count
                            
#                             logger.info(f"Received result {results_count}/{expected_count} for request {request_id}")
                            
#                             # If we've collected all results, we can stop
#                             if results_count >= expected_count:
#                                 break
            
#             # If we've collected all results, we can stop
#             if results_count >= expected_count:
#                 break
                
#         # Update status based on results
#         with task_locks.get(request_id, threading.Lock()):
#             if results_count >= expected_count:
#                 task_results[request_id]["status"] = "completed"
#             else:
#                 task_results[request_id]["status"] = "timeout"
#                 logger.warning(f"Timeout waiting for results for request {request_id}. "
#                               f"Got {results_count}/{expected_count} results.")
                
#     except Exception as e:
#         logger.error(f"Error collecting results: {e}", exc_info=True)
#         with task_locks.get(request_id, threading.Lock()):
#             task_results[request_id]["status"] = "error"
#             task_results[request_id]["error"] = str(e)

# @app.route("/health", methods=["GET"])
# def health_check():
#     """Health check endpoint"""
#     try:
#         # Check Redis connection
#         app.config["SESSION_REDIS"].ping()
        
#         # Check Kafka connection
#         producer = create_kafka_producer()
#         producer.close()
        
#         return jsonify({
#             "status": "healthy",
#             "timestamp": time.time(),
#             "components": {
#                 "redis": "connected",
#                 "kafka": "connected"
#             }
#         }), 200
#     except Exception as e:
#         logger.error(f"Health check failed: {e}", exc_info=True)
#         return jsonify({
#             "status": "unhealthy",
#             "timestamp": time.time(),
#             "error": str(e)
#         }), 500

# @app.route("/execute_training", methods=["POST"])
# def execute_training():
#     """
#     Synchronous endpoint to execute distributed training and return best results.
    
#     Expected JSON payload:
#     {
#         "dataset_id": "https://www.kaggle.com/datasets/owner/dataset-name",
#         "algorithm": "XGBoost",
#         "hyperparameters_list": [
#             {"n_estimators": "100", "max_depth": "3", "learning_rate": "0.1"},
#             {"n_estimators": "200", "max_depth": "5", "learning_rate": "0.05"}
#         ],
#         "target_column": "target"  # Optional
#     }
#     """
#     try:
#         data = request.get_json()
        
#         # Validate required fields
#         required_fields = ["dataset_id", "algorithm", "hyperparameters_list"]
#         for field in required_fields:
#             if field not in data:
#                 return jsonify({"error": f"Missing required field: {field}"}), 400
        
#         # Generate a unique request ID
#         request_id = str(uuid.uuid4())
        
#         # Create tasks for each hyperparameter configuration
#         tasks = []
#         for hyperparams in data["hyperparameters_list"]:
#             task = {
#                 "request_id": request_id,
#                 "dataset_id": data["dataset_id"],
#                 "algorithm": data["algorithm"],
#                 "hyperparameters": hyperparams
#             }
            
#             # Add target column if specified
#             if "target_column" in data:
#                 task["target_column"] = data["target_column"]
                
#             tasks.append(task)
        
#         # Distribute tasks to workers
#         producer = create_kafka_producer()
#         for task in tasks:
#             producer.send("task_requests", task)
#         producer.flush()
        
#         logger.info(f"Submitted {len(tasks)} tasks for request {request_id}")
        
#         # Collect results synchronously
#         consumer = create_kafka_consumer()
        
#         results = []
#         timeout = time.time() + 300  # 5-minute timeout
#         expected_count = len(tasks)
        
#         while time.time() < timeout and len(results) < expected_count:
#             # Poll with a timeout of 1 second
#             messages = consumer.poll(timeout_ms=1000)
            
#             for topic_partition, records in messages.items():
#                 for record in records:
#                     result = record.value
                    
#                     # Check if this result belongs to our request
#                     if result.get("request_id") == request_id:
#                         results.append(result)
#                         logger.info(f"Received result {len(results)}/{expected_count} for request {request_id}")
                        
#             if len(results) >= expected_count:
#                 break
        
#         # If we didn't get any results, return an error
#         if not results:
#             return jsonify({"error": "No results received from workers"}), 500
        
#         # Find the best model based on accuracy
#         best_result = None
#         best_accuracy = -1
        
#         for result in results:
#             if "accuracy" in result and result.get("accuracy", 0) > best_accuracy:
#                 best_accuracy = result.get("accuracy", 0)
#                 best_result = result
        
#         if best_result is None:
#             return jsonify({"error": "No valid results returned from workers"}), 500
        
#         # Return simplified response with just the best model
#         return jsonify({
#             "algorithm": data["algorithm"],
#             "best_accuracy": best_accuracy,
#             "best_hyperparameters": best_result.get("hyperparameters", {})
#         }), 200
        
#     except Exception as e:
#         logger.error(f"Error processing training request: {e}", exc_info=True)
#         return jsonify({"error": str(e)}), 500

# @app.route("/training_status/<request_id>", methods=["GET"])
# def training_status(request_id):
#     """Get the status of a training request"""
#     if request_id not in task_results:
#         return jsonify({"error": "Request ID not found"}), 404
    
#     status = task_results[request_id]["status"]
#     completed = task_results[request_id]["completed"]
#     expected = task_results[request_id]["expected"]
    
#     response = {
#         "request_id": request_id,
#         "status": status,
#         "progress": f"{completed}/{expected}"
#     }
    
#     # If training is complete, include the results
#     if status == "completed":
#         results = task_results[request_id]["results"]
        
#         # Find the best model based on accuracy
#         best_result = max(results, key=lambda x: x.get("accuracy", 0))
        
#         response["results"] = {
#             "best_model": {
#                 "algorithm": best_result.get("algorithm", "unknown"),
#                 "hyperparameters": best_result.get("hyperparameters", {}),
#                 "accuracy": best_result.get("accuracy", 0),
#                 "worker_id": best_result.get("worker_id", "unknown")
#             },
#             "all_results": results
#         }
        
#     elif status == "error":
#         response["error"] = task_results[request_id].get("error", "Unknown error")
        
#     return jsonify(response)

# @app.route("/clear_results/<request_id>", methods=["DELETE"])
# def clear_results(request_id):
#     """Clear results for a specific request ID"""
#     if request_id in task_results:
#         del task_results[request_id]
#         if request_id in task_locks:
#             del task_locks[request_id]
#         return jsonify({"message": f"Results for request {request_id} cleared"}), 200
#     else:
#         return jsonify({"error": "Request ID not found"}), 404

# if __name__ == "__main__":
#     # Start the Flask app
#     app.run(host="0.0.0.0", port=5001)




import os
import time
import uuid
import json
import logging
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from flask_cors import CORS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

def create_kafka_producer():
    """Create and return a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        raise

def create_kafka_consumer():
    """Create and return a Kafka consumer for the task_results topic."""
    try:
        consumer = KafkaConsumer(
            'task_results',
            bootstrap_servers=['kafka:9092'],
            group_id='master_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {e}")
        raise

def distribute_tasks(dataset_id, algorithm, hyperparameters_list, target_column=None):
    """
    Distribute tasks to worker nodes via Kafka.
    Returns a request ID for tracking.
    """
    try:
        # Generate a request ID
        request_id = str(uuid.uuid4())
        
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Send a task for each set of hyperparameters
        for hyperparameters in hyperparameters_list:
            task = {
                "request_id": request_id,
                "dataset_id": dataset_id,
                "algorithm": algorithm,
                "hyperparameters": hyperparameters,
            }
            
            # Add target_column if specified
            if target_column is not None:
                task["target_column"] = target_column
            
            producer.send('task_requests', task)
            logger.info(f"Sent task: {task}")
        
        producer.flush()
        return request_id
    except Exception as e:
        logger.error(f"Error distributing tasks: {e}")
        raise

def collect_results(request_id, timeout=60):
    """
    Collect results from worker nodes via Kafka.
    Returns a list of results.
    """
    try:
        # Create Kafka consumer
        consumer = create_kafka_consumer()
        
        # Set timeout
        end_time = time.time() + timeout
        
        # Collect results
        results = []
        
        # Poll for messages until timeout
        while time.time() < end_time:
            # Poll with a timeout of 1 second
            messages = consumer.poll(timeout_ms=1000)
            
            # Process messages
            for topic_partition, msgs in messages.items():
                for message in msgs:
                    result = message.value
                    
                    # Check if this result is for our request
                    if result.get("request_id") == request_id:
                        results.append(result)
                        logger.info(f"Collected result: {result}")
            
            # Check if we have enough results (one for each hyperparameter set)
            if len(results) >= 1:  # We need at least one result
                break
        
        return results
    except Exception as e:
        logger.error(f"Error collecting results: {e}")
        raise

def find_best_result(results):
    """
    Find the best result based on the problem type.
    For classification, higher accuracy is better.
    For regression, lower RMSE is better.
    """
    if not results:
        return None
    
    # Determine the problem type from the first result
    problem_type = results[0].get('problem_type', 'classification')
    
    best_result = None
    
    if problem_type == 'classification':
        # For classification, higher accuracy is better
        best_metric = -1
        for result in results:
            if result.get('status') == 'success' and result.get('accuracy', 0) > best_metric:
                best_metric = result.get('accuracy', 0)
                best_result = result
    else:
        # For regression, lower RMSE is better
        best_metric = float('inf')
        for result in results:
            if result.get('status') == 'success' and result.get('rmse', float('inf')) < best_metric:
                best_metric = result.get('rmse', float('inf'))
                best_result = result
    
    return best_result

@app.route("/")
def home():
    """Home route for the API."""
    return jsonify({
        "message": "Distributed ML System API",
        "status": "running",
        "endpoints": [
            "/execute_training",
            "/health"
        ]
    })

@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": time.time()
    })

@app.route("/execute_training", methods=["POST"])
def execute_training():
    """
    Execute training on worker nodes.
    Expects a JSON payload with:
    - dataset_id: URL or identifier of the dataset
    - algorithm: ML algorithm to use
    - hyperparameters_list: List of hyperparameter sets to try
    - target_column: (Optional) Name or index of the target column
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ["dataset_id", "algorithm", "hyperparameters_list"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        dataset_id = data["dataset_id"]
        algorithm = data["algorithm"]
        hyperparameters_list = data["hyperparameters_list"]
        target_column = data.get("target_column")  # Optional
        
        # Validate algorithm
        supported_algorithms = ["RandomForest", "XGBoost", "KNN"]
        if algorithm not in supported_algorithms:
            return jsonify({"error": f"Unsupported algorithm: {algorithm}. Supported algorithms: {supported_algorithms}"}), 400
        
        # Validate hyperparameters_list
        if not isinstance(hyperparameters_list, list) or len(hyperparameters_list) == 0:
            return jsonify({"error": "hyperparameters_list must be a non-empty list"}), 400
        
        # Distribute tasks to worker nodes
        request_id = distribute_tasks(dataset_id, algorithm, hyperparameters_list, target_column)
        
        # Collect results from worker nodes
        results = collect_results(request_id)
        
        # Find the best result
        best_result = find_best_result(results)
        
        if best_result is None:
            return jsonify({"error": "No successful results received from workers"}), 500
        
        # Determine problem type
        problem_type = best_result.get('problem_type', 'classification')
        
        # Prepare response based on problem type
        if problem_type == 'classification':
            response = {
                "algorithm": algorithm,
                "best_accuracy": best_result.get("accuracy"),
                "best_hyperparameters": best_result.get("hyperparameters"),
            }
            if "f1_score" in best_result:
                response["f1_score"] = best_result["f1_score"]
        else:  # regression
            response = {
                "algorithm": algorithm,
                "best_rmse": best_result.get("rmse"),
                "r2_score": best_result.get("r2"),
                "best_hyperparameters": best_result.get("hyperparameters"),
            }
        
        return jsonify(response), 200
    
    except Exception as e:
        logger.error(f"Error executing training: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)