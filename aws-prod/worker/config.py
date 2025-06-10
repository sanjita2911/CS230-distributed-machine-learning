# AWS Configuration
#
# REGION = 'us-east-2'
# REDIS_ADDRESS = '172.31.11.144'
# SCHEDULER_ADDRESS =  TO ADD AWS ADDRESS

# Local Docker Configuration


REGION = 'us-east-2'
KAFKA_ADDRESS = 'kafka:9092'  # on docker only
KAFKA_TRAIN_TOPIC = 'train'
KAFKA_RESULTS_TOPIC = 'result'
REDIS_ADDRESS = 'redis'  # on docker only
SCHEDULER_ADDRESS =  "http://scheduler:8000"