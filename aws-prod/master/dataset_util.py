from datasets import load_dataset
import requests
import kaggle
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import yaml
import io
import shutil
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, MinMaxScaler
from pyspark.ml import Pipeline


def upload_to_s3(local_path, bucket):
    s3 = boto3.client("s3")
    for root, dirs, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, local_path)
            s3_path = os.path.join("raw-data", relative_path).replace('\\', '//')   # bucket = distributed-preprocessing/raw-data
            s3.upload_file(full_path, bucket, s3_path)


def download_dataset(url, source_type, save_path, s3_bucket=None, s3_prefix=None):
    try:
        os.makedirs(save_path, exist_ok=True)

        if source_type.lower() == "huggingface":
            dataset = load_dataset(url)
            dataset.save_to_disk(save_path)

        elif source_type.lower() == "kaggle":
            kaggle.api.dataset_download_files(url, path=save_path, unzip=True)

        elif source_type.lower() == "url":
            response = requests.get(url, stream=True)
            file_name = os.path.join(save_path, url.split("/")[-1])
            with open(file_name, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            return False, f"Invalid source type: {source_type}"

        if s3_bucket and s3_prefix:
            upload_to_s3(save_path, s3_bucket, s3_prefix)
            shutil.rmtree(save_path)  # clean up local copy

        return True, f"Dataset downloaded and uploaded to s3://{s3_bucket}/{s3_prefix}"
    except Exception as e:
        return False, str(e)
    


def create_spark_session(app_name='PreprocessingApp'):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.4.0 pyspark-shell'

    conf = SparkConf().\
    set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true').\
    set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true').\
    setAppName(app_name).setMaster('local[*]')
    sc = SparkContext(conf=conf)

    aws_access_key = os.environ.get('AWS_ACCESS_KEY')
    aws_secret_key = os.environ.get('AWS_SECRET_KEY')
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', aws_access_key)
    hadoopConf.set('fs.s3a.secret.key', aws_secret_key)
    hadoopConf.set('fs.s3a.endpoint', 's3-us-west-1.amazonaws.com')
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    spark = SparkSession(sc)
    return spark


def download_data_and_yaml(yaml_s3_path, data_s3_path):
    spark = create_spark_session()
    bucket, yaml_key = yaml_s3_path.replace("s3://", "").split("/", 1)
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=yaml_key)
    config = yaml.safe_load(obj['Body'].read())
    df = spark.read.csv(data_s3_path, header=True, inferSchema=True)

    return df, config


def preprocess_data(df, config):
    
    pipeline_stages = []

    # 1. Drop null rows
    if config.get("drop_null", False):
        df = df.na.drop()

    # 2. Impute missing values
    if "impute" in config:
        for col, strategy in config["impute"].items():
            if strategy == "mean":
                mean_val = df.select(F.mean(F.col(col))).first()[0]
                df = df.fillna({col: mean_val})
            elif strategy == "median":
                median_val = df.approxQuantile(col, [0.5], 0.01)[0]
                df = df.fillna({col: median_val})
            elif strategy == "mode":
                mode_val = df.groupBy(col).count().orderBy(F.desc("count")).first()[0]
                df = df.fillna({col: mode_val})
            elif isinstance(strategy, (int, float, str)):
                df = df.fillna({col: strategy})

    # 3. Outlier detection + handling
    if "outliers" in config:
        for col, method in config["outliers"].items():
            if method == "remove":
                q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.05)
                iqr = q3 - q1
                lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                df = df.filter((F.col(col) >= lower) & (F.col(col) <= upper))
            elif method == "clip":
                q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.05)
                iqr = q3 - q1
                lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                df = df.withColumn(col, F.when(F.col(col) < lower, lower).when(F.col(col) > upper, upper).otherwise(F.col(col)))

    # 4. Drop columns
    if "drop_columns" in config:
        df = df.drop(*config["drop_columns"])

    # 5. Drop duplicates
    if config.get("drop_duplicates", False):
        df = df.dropDuplicates()

    # 6. Categorical encoding
    if "categorical" in config:
        for col in config["categorical"]:
            indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid='keep')
            encoder = OneHotEncoder(inputCols=[f"{col}_index"], outputCols=[f"{col}_encoded"])
            pipeline_stages += [indexer, encoder]

    # 7. Feature scaling
    if "scale" in config:
        input_cols = config["scale"].get("columns", [])
        output_cols = [f"{col}_scaled" for col in input_cols]
        assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

        if config["scale"]["method"] == "standard":
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
        else:
            scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")

        pipeline_stages += [assembler, scaler]

    # Fit and transform if needed
    if pipeline_stages:
        pipeline = Pipeline(stages=pipeline_stages)
        model = pipeline.fit(df)
        df = model.transform(df)

    return df