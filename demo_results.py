import requests
import matplotlib.pyplot as plt
import numpy as np

# fill in after running demo_tests
session_id = "4aaa40e9-33cc-42b0-9ed5-43db061cb7dd"
job1_id = "f824720a-fa74-4932-900b-c467d09fbb47"
job2_id = "7625ab0c-15fd-4dfe-aa10-a51b75fee5c4"
job3_id = "5a316d7f-7c2a-4014-b490-6b3be6afe5ca"

# training results
job_response = requests.get(f"http://3.17.175.226:5001/check_status/{session_id}/{job1_id}").json()
print("TRAINING RESULTS:")
print("Average accuracy:", round(job_response.get('job_result')['best_result']['mean_cv_score'] * 100, 4), '%')
total_time1 = 0
for result in job_response.get('job_result')['results']:
    total_time1 += result["training_time"]
print("Total training time:", round(total_time1, 4), "seconds")
print("Total subtasks:", job_response.get('total_subtasks'), '\n\n')



# gridsearch results
job_response = requests.get(f"http://3.17.175.226:5001/check_status/{session_id}/{job2_id}").json()
print("GRIDSEARCHCV RESULTS:")
print("Average accuracy:", round(job_response['job_result']['best_result']['mean_cv_score'] * 100, 4), '%')
total_time2 = 0
for result in job_response.get('job_result')['results']:
    total_time2 += result["training_time"]
print("Total tuning time:", round(total_time2, 4), "seconds")
print("Best parameters:", job_response.get('job_result')['best_result']['parameters'])
print("Total subtasks:", job_response.get('total_subtasks'), '\n\n')



# randomizedsearchcv results
job_response = requests.get(f"http://3.17.175.226:5001/check_status/{session_id}/{job3_id}").json()
print("RANDOMIZEDSEARCHCV RESULTS:")
print("Average accuracy:", round(job_response['job_result']['best_result']['mean_cv_score'] * 100, 4), '%')
total_time3 = 0
for result in job_response.get('job_result')['results']:
    total_time3 += result["training_time"]
print("Total tuning time:", round(total_time3, 4), "seconds")
print("Best parameters:", job_response.get('job_result')['best_result']['parameters'])
print("Total subtasks:", job_response.get('total_subtasks'))



# RESULTS GRAPH 1
methods = ["Training", "GridSearchCV", "RandomizedSearchCV"]

distributed_times = [total_time1, total_time2, total_time3]
scikit_times = [0.036, 8.433, 0.5843] 

bar_width = 0.35
x = np.arange(len(methods))

fig, ax = plt.subplots(figsize=(8, 6))
ax.bar(x - bar_width/2, scikit_times, bar_width, label="scikit-learn", color='gray')
ax.bar(x + bar_width/2, distributed_times, bar_width, label="distributed-ml", color='blue')

ax.set_ylabel("Time (seconds)")
ax.set_title("Distributed vs. Non-Distributed Runtimes")
ax.set_xticks(x)
ax.set_xticklabels(methods)
ax.legend()

plt.show()