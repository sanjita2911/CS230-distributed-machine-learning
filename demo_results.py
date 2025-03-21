import requests


# fill in after running demo_tests
session_id = "94d5dde4-14ad-4528-9835-f822843f252c"
job1_id = "4f69faaf-ae71-4411-9bb0-5333ceef8810"
job2_id = "0b2a9806-5ca1-47fd-9470-66f4d4ef98f2"
job3_id = "e8726205-d186-4864-9a58-a8d00337bbfa"

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