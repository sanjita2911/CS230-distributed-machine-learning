# Worker Node Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy and install dependencies
COPY docker/requirements_worker.txt .
RUN pip install -r requirements_worker.txt

# Copy application files
COPY worker .

# Expose gRPC port
#EXPOSE 50051

# Run the worker node server
CMD ["python", "worker.py"]
