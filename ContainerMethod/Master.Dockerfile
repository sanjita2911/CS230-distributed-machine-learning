FROM python:3.9-slim

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application files
COPY master_node.py ml_task_pb2.py ml_task_pb2_grpc.py .

# Expose Flask port
EXPOSE 5000

# Run the master node server
CMD ["python", "master_node.py"]
