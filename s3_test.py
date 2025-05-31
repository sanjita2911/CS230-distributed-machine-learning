import os
import findspark
# findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# os.environ["SPARK_HOME"] = "/Library/spark-4.0.0-bin-hadoop3"
# os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-17.0.15.jdk/Contents/Home"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.4.0 pyspark-shell'

conf = SparkConf().\
set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true').\
set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true').\
setAppName('read_data_from_s3_example').setMaster('local[*]')
sc = SparkContext(conf=conf)

aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', aws_access_key)
hadoopConf.set('fs.s3a.secret.key', aws_secret_key)
hadoopConf.set('fs.s3a.endpoint', 's3-us-west-1.amazonaws.com')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

spark = SparkSession(sc)
iris = spark.read.csv('s3a://distributed-preprocessing/Iris.csv', header=True, inferSchema=True)
iris.printSchema()