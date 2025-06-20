from flask import Flask, request, jsonify, send_file, Response, stream_with_context
from flask_cors import CORS  # Import CORS
import time
import random
import os
import threading
import glob
import json
import uuid
from config import DATASET_PATH, KAFKA_ADDRESS, CONFIG_PATH
from dataset_util import download_dataset, preprocess_data, collect_csv_metadata
from logger_util import logger
from redis_util import create_redis_client, save_subtasks_to_redis, update_subtask, save_job_metadata
from task_handler import create_subtasks, start_result_collector, consume_results
from kafka_util import KafkaSingleton, send_to_kafka, get_consumer
from kafka import TopicPartition
from kafka import KafkaConsumer
import yaml

redis_client = create_redis_client()


app = Flask(__name__)
CORS(app)  # Enable CORS for all routes


@app.route("/")
def home():
    """Home route for the API."""
    return jsonify({
        "message": "Distributed ML System API",
        "status": "running",
        "endpoints": [
            "/health",
            "/create_session",
            "/download_data/<session_id>",
            "/check_data/<session_id>",
            "/train/<session_id>",
            "/train_status/<session_id>",
            "/metrics/<session_id>/<job_id>"
            "/download_model/<session_id>/<job_id>",
            "/preprocess/<session_id>"
        ]
    })


@app.route("/health", methods=["POST"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": time.time()
    })


@app.route("/create_session", methods=["POST"])
def create_session():
    """Creates a new user session and stores it in Redis."""
    session_id = str(uuid.uuid4())
    redis_client.sadd("active_sessions", session_id)  # Add to active sessions
    return jsonify({"message": "Session created", "session_id": session_id}), 201


@app.route('/download_data/<session_id>', methods=['POST'])
def download_data(session_id):

    # Validate session ID
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404

    data = request.get_json()
    # Validate dataset parameters
    url = data.get("dataset_url")
    name = data.get("dataset_name")
    source_type = data.get("dataset_type")

    if not url or not name or not source_type:
        return jsonify({"error": "Missing dataset parameters"}), 400

    dataset_path = os.path.join(DATASET_PATH, name)

    # Download the dataset synchronously
    success, message = download_dataset(url, source_type, dataset_path)

    if success:
        return jsonify({"message": message}), 200
    else:
        return jsonify({"error": message}), 500


@app.route('/check_data/<session_id>', methods=['GET'])
def check_data(session_id):
    """Check the status of dataset download."""

    # Validate session ID
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404
    dataset_id = request.args.get("dataset_name")
    # data = request.get_json()
    # dataset_id = data.get('dataset_name')
    # dataset_path = f"/mnt/efs/datasets/{dataset_id}/{dataset_id}.csv"  # On AWS Comment
    # dataset_path = f"/mnt/datasets/{dataset_id}.csv" # ON AWS uncomment

    data_dir = f"/mnt/efs/datasets/{dataset_id}"
    csv_paths = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    dataset_path = csv_paths[0]
    if os.path.exists(dataset_path):
        return jsonify({'status': f'Dataset {dataset_id} found at {dataset_path}'}), 200
    else:
        return jsonify({'error': f'Dataset {dataset_id} not found, Please Use download_data function'}), 404


@app.route('/check_status/<session_id>/<job_id>', methods=['GET'])
def check_status(session_id, job_id):
    """Returns the number of pending tasks for a session, job status, and job results if completed."""

    # Check if the session is valid
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404

    session_key = f"active_sessions:{session_id}"
    job_key = f"{session_key}:jobs:{job_id}"

    # Retrieve the status (completion percentage)
    job_status = redis_client.hget(job_key, "status")
    if not job_status:
        return jsonify({"error": f"Job {job_id} not found."}), 404
    # Get pending tasks for the session
    pending_tasks = int(redis_client.get(
        f"session:{session_id}:tasks_pending") or 0)

    # Get the total number of subtasks for the job
    subtask_keys = redis_client.keys(
        f"active_sessions:{session_id}:jobs:{job_id}:subtasks:{job_id}-subtask-*")
    total_subtasks = len(subtask_keys)

    # If the job is completed, fetch the job results
    if job_status == "completed":
        job_result = redis_client.get(f"{job_key}:result")
        if not job_result:
            return jsonify({"error": "Job result not found."}), 404

        job_result = json.loads(job_result)
        response = {"session_id": session_id,
                    "tasks_pending": pending_tasks,
                    "job_id": job_id,
                    "job_status": job_status,
                    "job_result": job_result,  # Include job results in the response
                    "total_subtasks": total_subtasks  # Total subtasks for the status bar
                    }
        if total_subtasks > 1:
            response['best_result'] = job_result.get('best_result')
            logger.info(job_result)
            return jsonify(response)

        return jsonify(response)

    # If the job is not completed, return job status and pending tasks
    return jsonify({
        "session_id": session_id,
        "tasks_pending": pending_tasks,
        "job_id": job_id,
        "job_status": job_status,
        "total_subtasks": total_subtasks  # Total subtasks for the status bar
    })


@app.route('/train/<session_id>', methods=['POST'])
def train(session_id):

    producer = KafkaSingleton.get_producer()

    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404
    # get model details
    model_config = request.get_json()
    dataset_id = model_config.get('dataset_id')
    # dataset_path = f"/mnt/efs/datasets/{dataset_id}/{dataset_id}.csv"  # On AWS Comment
    # dataset_path = f"/mnt/datasets/{dataset_id}.csv" # ON AWS uncomment

    data_dir = f"/mnt/efs/datasets/{dataset_id}"
    csv_paths = glob.glob(os.path.join(data_dir, "*.csv"))
    dataset_path = csv_paths[0]

    if not os.path.exists(dataset_path):
        return jsonify({'error': f'Dataset {dataset_id} not found, Please Use download_data function'}), 404
    # model_details = model_config['model_details']
    # data_config = model_config.get('data_config', {})
    logger.info(model_config)
    subtasks = create_subtasks(model_config)
    subtask_list = [task['subtask_id'] for task in subtasks]
    logger.info(subtasks)
    metadata = collect_csv_metadata(dataset_path)
    save_job_metadata(session_id, subtasks[0]['job_id'], metadata, redis_client)
    redis_status = save_subtasks_to_redis(subtasks, redis_client)
    logger.info(redis_status)
    send_to_kafka(subtasks, producer)

    listener_thread = threading.Thread(
        target=consume_results, args=(subtask_list, session_id), daemon=True)
    listener_thread.start()
    logger.info("Result collector thread started here...")

    return jsonify({"status": "Model Training Started . . . ."})


@app.route('/train_status/<session_id>', methods=['POST'])
def train_status(session_id):
    producer = KafkaSingleton.get_producer()

    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404

    model_config = request.get_json()
    dataset_id = model_config.get('dataset_id')
    data_dir = f"/mnt/efs/datasets/{dataset_id}"
    csv_paths = glob.glob(os.path.join(data_dir, "*.csv"))
    dataset_path = csv_paths[0] if csv_paths else None

    if not dataset_path or not os.path.exists(dataset_path):
        return jsonify({'error': f'Dataset {dataset_id} not found'}), 404

    subtasks = create_subtasks(model_config)
    subtask_ids = [task['subtask_id'] for task in subtasks]
    redis_status = save_subtasks_to_redis(subtasks, redis_client)
    send_to_kafka(subtasks, producer)

    # Start consumer thread (optional, if background needed)
    listener_thread = threading.Thread(
        target=consume_results, args=(subtask_ids, session_id), daemon=True)
    listener_thread.start()
    logger.info("Result collector thread started here...")

    # Stream progress + result
    def event_stream():
        job_id = subtasks[0]['job_id']
        job_key = f"active_sessions:{session_id}:jobs:{job_id}"
        result_key = f"{job_key}:result"

        while True:
            job_status = redis_client.hget(job_key, "status")
            pending_tasks = int(redis_client.get(f"session:{session_id}:tasks_pending") or 0)
            subtask_keys = redis_client.keys(f"{job_key}:subtasks:{job_id}-subtask-*")
            total_subtasks = len(subtask_keys)

            data = {
                "session_id": session_id,
                "job_id": job_id,
                "job_status": job_status if job_status else "unknown",
                "tasks_pending": pending_tasks,
                "total_subtasks": total_subtasks,
            }

            # If complete, send final result
            if job_status == "completed":
                result = redis_client.get(result_key)
                if result:
                    data["job_result"] = json.loads(result)
                yield f"data: {json.dumps(data)}\n\n"
                break

            # Send status update
            yield f"data: {json.dumps(data)}\n\n"
            time.sleep(1.5)

    return Response(stream_with_context(event_stream()), mimetype='text/event-stream')

@app.route('/download_model/<session_id>/<job_id>', methods=['POST'])
def download_best_model(session_id, job_id):
    # Simulate fetching the best result for a given job_id
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404

    session_key = f"active_sessions:{session_id}"
    job_key = f"{session_key}:jobs:{job_id}"

    # Retrieve the status (completion percentage)
    job_status = redis_client.hget(job_key, "status")
    if not job_status:
        return jsonify({"error": f"Job {job_id} not found."}), 404

    data = request.get_json()
    # Check if the model path exists
    model_path = data['model_path']
    if not os.path.exists(model_path):
        return jsonify({"status": "error", "message": "Model file not found"}), 404

    # Return the model file for download
    return send_file(model_path, as_attachment=True, attachment_filename=f"{data['model_id']}.pkl")


@app.route('/metrics/<session_id>/<job_id>', methods=['GET'])
def stream_metrics(session_id, job_id):
    # 1) Validate the session
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session"}), 404

    # 2) Look up all subtask IDs for this job in Redis
    prefix = f"active_sessions:{session_id}:jobs:{job_id}:subtasks:{job_id}-subtask-"
    keys = redis_client.keys(prefix + "*")
    subtask_ids = [k.split(":")[-1] for k in keys]
    if not subtask_ids:
        return jsonify({"error": "No subtasks found"}), 404

    # 3) Spin up a fresh Kafka consumer group so we read every metric ever produced
    group_name = f"metrics_{session_id}_{job_id}"
    kafka_address = KAFKA_ADDRESS  # however you configure it
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_address],
        group_id=group_name,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # 4) Explicitly assign all partitions and rewind to the start
    parts = [TopicPartition('metrics', p)
             for p in consumer.partitions_for_topic('metrics')]
    consumer.assign(parts)
    consumer.seek_to_beginning()

    # 5) Consume messages until we’ve collected one record per subtask
    seen = {}
    for msg in consumer:
        record = msg.value
        sid = record['subtask_id']
        if sid in subtask_ids and sid not in seen:
            seen[sid] = record
            if len(seen) == len(subtask_ids):
                break

    consumer.close()

    with open("metrics.json", "w") as f:
        json.dump(list(seen.values()), f, indent=4)

    # 6) Return the collected metrics as a JSON array
    return jsonify(list(seen.values())), 200


@app.route('/preprocess/<session_id>', methods=['POST'])
def preprocess(session_id):
    # Validate session ID
    if not redis_client.sismember("active_sessions", session_id):
        return jsonify({"error": "Invalid session ID"}), 404
    
    payload = request.get_json()
    
    # Retrieve data
    dataset_id = payload.get('dataset_id')
    data_dir = f"/mnt/efs/datasets/{dataset_id}"
    csv_paths = glob.glob(os.path.join(data_dir, "*.csv"))
    dataset_path = csv_paths[0] if csv_paths else None
    
    if not dataset_path or not os.path.exists(dataset_path):
        return jsonify({'error': f'Dataset {dataset_id} not found'}), 404
    
    # Upload yaml to efs   
    yaml_url = payload.get('yaml_url')
    yaml_dir = os.path.join(CONFIG_PATH, dataset_id)   # dataset and yaml under same id

    if not yaml_url:
        return jsonify({"error": "Missing yaml parameters"}), 400
    
    # success, message = download_dataset(yaml_url, "local", yaml_dir)
    # if not success:
    #     return jsonify({"error": message}), 500
    
    # Retrieve yaml
    yaml_paths = glob.glob(os.path.join(yaml_dir, "*.yaml"))
    yaml_path = yaml_paths[0] if yaml_paths else None
    
    # Preprocess data w/ yaml settings
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)

    preprocessed_df = preprocess_data(dataset_path, config)

    # Upload preprocessed data as csv to efs
    preprocessed_data_dir = os.path.join(data_dir, "preprocessed")
    preprocessed_data_url = f"{dataset_id}_preprocessed.csv"
    preprocessed_df.to_csv(preprocessed_data_url, index=False)

    success, message = download_dataset(preprocessed_data_url, "local", preprocessed_data_dir)
    if success:
        return jsonify({"message": f"Dataset successfully preprocessed and downloaded to {preprocessed_data_dir}"}), 200
    else:
        return jsonify({"error": message}), 500


if __name__ == "__main__":
    logger.info("Starting Flask server...")

    app.run(debug=True, host="0.0.0.0", port=5001)
