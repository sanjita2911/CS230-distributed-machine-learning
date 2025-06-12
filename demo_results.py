import requests
import matplotlib.pyplot as plt
import numpy as np

# fill in after running demo_tests
session_id = "f6ae8ec0-fd73-4104-8415-02e89707290d"
job1_id = "99947d24-edca-43de-915a-866150ee0920"
# job2_id = "914cbeb9-6391-47be-9fc9-4ec3b8fd7fd0"
# job3_id = "c1c27acb-5c1b-4dd2-8954-3e4ab9531f8d"

# training results
job_response = requests.get(f"http://127.0.0.1:5001/metrics/{session_id}/{job1_id}").json()
print("TRAINING RESULTS:")
print("Average accuracy:", round(job_response.get('job_result')['best_result']['mean_cv_score'] * 100, 4), '%')
total_time1 = 0
for result in job_response.get('job_result')['results']:
    total_time1 += result["training_time"]
print("Total training time:", round(total_time1, 4), "seconds")
print("Total subtasks:", job_response.get('total_subtasks'), '\n\n')



# # gridsearch results
# job_response = requests.get(f"http://127.0.0.1:5001/metrics/{session_id}/{job2_id}").json()
# print("GRIDSEARCHCV RESULTS:")
# print("Average accuracy:", round(job_response['job_result']['best_result']['mean_cv_score'] * 100, 4), '%')
# total_time2 = 0
# for result in job_response.get('job_result')['results']:
#     total_time2 += result["training_time"]
# print("Total tuning time:", round(total_time2, 4), "seconds")
# print("Best parameters:", job_response.get('job_result')['best_result']['parameters'])
# print("Total subtasks:", job_response.get('total_subtasks'), '\n\n')



# # randomizedsearchcv results
# job_response = requests.get(f"http://127.0.0.1:5001/metrics/{session_id}/{job3_id}").json()
# print("RANDOMIZEDSEARCHCV RESULTS:")
# print("Average accuracy:", round(job_response['job_result']['best_result']['mean_cv_score'] * 100, 4), '%')
# total_time3 = 0
# for result in job_response.get('job_result')['results']:
#     total_time3 += result["training_time"]
# print("Total tuning time:", round(total_time3, 4), "seconds")
# print("Best parameters:", job_response.get('job_result')['best_result']['parameters'])
# print("Total subtasks:", job_response.get('total_subtasks'))



# # RESULTS GRAPH 1
# methods = ["Training", "GridSearchCV", "RandomizedSearchCV"]

# distributed_times = [total_time1, total_time2, total_time3]
# scikit_times = [0.036, 8.433, 0.5843] 

# bar_width = 0.35
# x = np.arange(len(methods))

# fig, ax = plt.subplots(figsize=(8, 6))
# ax.bar(x - bar_width/2, scikit_times, bar_width, label="scikit-learn", color='gray')
# ax.bar(x + bar_width/2, distributed_times, bar_width, label="distributed-ml", color='blue')

# ax.set_ylabel("Time (seconds)")
# ax.set_title("Distributed vs. Non-Distributed Runtimes")
# ax.set_xticks(x)
# ax.set_xticklabels(methods)
# ax.legend()

# plt.show()