import requests


# fill in after running demo_tests
session_id = "c3fba33e-25db-405c-917f-4c694e22b95a"
job1_id = "6853f494-9cb2-436c-bc09-0b01757ed088"
job2_id = "d4a7e527-d767-4ce1-87e6-58ea8226c6f7"
job3_id = "5111623a-6f50-4251-81fc-641a22f7d3ae"

# training results
job_response = requests.get(f"http://18.217.27.9:5001/check_status/{session_id}/{job1_id}").json()
print("TRAINING RESULTS:")
print("Average accuracy:", round(job_response.get('job_result')['best_result']['mean_cv_score'] * 100, 4), '%')
total_time = 0
for result in job_response.get('job_result')['results']:
    total_time += result["training_time"]
print("Total training time:", round(total_time, 4), "seconds")
print("Total subtasks:", job_response.get('total_subtasks'), '\n\n')



# gridsearch results
job_response = requests.get(f"http://18.217.27.9:5001/check_status/{session_id}/{job2_id}").json()
print("GRIDSEARCHCV RESULTS:")
print("Average accuracy:", round(job_response['job_result']['best_result']['mean_cv_score'] * 100, 4), '%')
total_time = 0
for result in job_response.get('job_result')['results']:
    total_time += result["training_time"]
print("Total tuning time:", round(total_time, 4), "seconds")
print("Best parameters:", job_response.get('job_result')['best_result']['parameters'])
print("Total subtasks:", job_response.get('total_subtasks'), '\n\n')



# randomizedsearchcv results
job_response = requests.get(f"http://18.217.27.9:5001/check_status/{session_id}/{job3_id}").json()
print("RANDOMIZEDSEARCHCV RESULTS:")
print("Average accuracy:", round(job_response['job_result']['best_result']['mean_cv_score'] * 100, 4), '%')
total_time = 0
for result in job_response.get('job_result')['results']:
    total_time += result["training_time"]
print("Total tuning time:", round(total_time, 4), "seconds")
print("Best parameters:", job_response.get('job_result')['best_result']['parameters'])
print("Total subtasks:", job_response.get('total_subtasks'))