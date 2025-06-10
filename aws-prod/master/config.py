# AWS Configuration

# DATASET_PATH = "/mnt/efs/datasets"
# REGION = 'us-east-2'
# REDIS_ADDRESS = '172.31.11.144'
# KAFKA_TRAIN_TOPIC = 'train'
# KAFKA_RESULTS_TOPIC = 'result'

# Local Docker Configuration

DATASET_PATH = "/mnt/efs/datasets"
REGION = 'us-east-2'
KAFKA_TRAIN_TOPIC = 'train'
KAFKA_RESULTS_TOPIC = 'result'
KAFKA_SCHEDULER_TASKS_TOPIC = 'tasks'
KAFKA_ADDRESS = 'kafka:9092'  # on docker only
REDIS_ADDRESS = 'redis'  # on docker only
