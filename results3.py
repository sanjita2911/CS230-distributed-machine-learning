import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from distributed_ml import MLTaskManager
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import kagglehub

task_manager = MLTaskManager()

# Download the dataset
download_status = task_manager.download_data("sahilprajapati143/retail-analysis-large-dataset", "large", "kaggle")
print(download_status)

# Verify the dataset
data_status = task_manager.check_data("large")
print(data_status)
 
data_sizes = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0]  # 1% to 100% of the data
times = []


for size in data_sizes:
    rf = RandomForestClassifier()

    start_time = time.time()
    job_response = task_manager.train(
        rf,
        dataset_name="large",
        train_params={
            "test_size": 0.2,
            "random_state": 42,
            "feature_columns": ['Customer_ID', 'Name', 'Email', 'Phone', 'Address', 'City', 'State', 'Zipcode', 'Country', 'Age', 'Gender', 'Income', 'Total_Purchases', 'Total_Amount', 'Ratings'],
            "target_column": "Customer_Segment",
        },
        wait_for_completion=True
    )
    elapsed_time = time.time() - start_time
    times.append(elapsed_time)

    print(f"✅ Trained on {size*100:.0f}% of data in {elapsed_time:.2f} seconds")

plt.figure(figsize=(8, 6))
plt.plot(np.array(data_sizes) * 100, times, marker="o", linestyle="-", color="blue")
plt.xlabel("Dataset Size (%)")
plt.ylabel("Training Time (seconds)")
plt.title("Distributed Training Time vs. Dataset Size (Distributed-ML)")
plt.grid()
plt.show()