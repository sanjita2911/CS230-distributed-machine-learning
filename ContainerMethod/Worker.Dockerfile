# Worker Node Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements_worker.txt .
RUN pip install -r requirements_worker.txt

# Copy application files
COPY worker_node.py ml_task_pb2.py ml_task_pb2_grpc.py .

# Expose gRPC port
EXPOSE 50051

# Run the worker node server
CMD ["python", "worker_node.py"]
