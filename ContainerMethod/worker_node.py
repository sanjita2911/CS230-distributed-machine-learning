import grpc
from concurrent import futures
import time
import ml_task_pb2
import ml_task_pb2_grpc
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import json
import random


# Simulated dataset
def generate_dataset(dataset_id):
    """
    Simulate loading a dataset based on the dataset_id.
    """
    # For simplicity, we use random data here
    # You can replace this with actual dataset loading logic
    if dataset_id == "dataset1":
        X = np.random.rand(100, 10)  # 100 samples, 10 features
        y = np.random.randint(0, 2, size=100)  # Binary target variable
    else:
        X = np.random.rand(100, 5)  # 100 samples, 5 features for other datasets
        y = np.random.randint(0, 2, size=100)

    return X, y


# Implement the gRPC service
class MLTaskService(ml_task_pb2_grpc.MLTaskServiceServicer):

    def ExecuteTask(self, request, context):
        """
        Handle incoming task execution request.
        Perform training with the specified algorithm and return results.
        """
        print(
            f"Received task: {request.algorithm} on dataset {request.dataset_id} with hyperparameters {request.hyperparameters}")

        # Load the dataset
        X, y = generate_dataset(request.dataset_id)

        # Extract hyperparameters
        n_estimators = int(request.hyperparameters.get("n_estimators", 100))
        max_depth = int(request.hyperparameters.get("max_depth", 5))

        # Train a RandomForest model (or other models as specified)
        if request.algorithm == "RandomForest":
            model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth)
            model.fit(X, y)

            # Simulate predictions and evaluation
            y_pred = model.predict(X)
            accuracy = accuracy_score(y, y_pred)

            result = f"RandomForest trained with accuracy: {accuracy:.2f}"
        else:
            result = "Unsupported algorithm"

        # Simulate processing time
        time.sleep(random.uniform(1, 3))

        # Create the response message
        response = ml_task_pb2.TaskResponse(
            worker_id="worker1",  # You can use dynamic worker IDs if needed
            result=result,
            execution_time=random.uniform(1, 3)  # Random execution time
        )

        return response


# Start the gRPC server
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ml_task_pb2_grpc.add_MLTaskServiceServicer_to_server(MLTaskService(), server)
    server.add_insecure_port('[::]:50051')  # Listen on port 50051
    print("Worker node started, waiting for tasks...")
    server.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
