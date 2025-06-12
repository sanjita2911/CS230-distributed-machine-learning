from datasets import load_dataset
import requests
import kaggle
import os
import boto3
import io
import csv
import shutil
import pandas as pd


def download_dataset(url, source_type, save_path, s3_bucket=None, s3_prefix=None):
    try:
        os.makedirs(save_path, exist_ok=True)

        if source_type.lower() == "huggingface":
            dataset = load_dataset(url)
            dataset.save_to_disk(save_path)

        elif source_type.lower() == "kaggle":
            kaggle.api.dataset_download_files(url, path=save_path, unzip=True)

        elif source_type.lower() == "local":
            if not os.path.isfile(url):
                return False, f"Local file not found at path: {url}"

            shutil.copy(url, save_path)
            print(f"Copied local file from {url} to {save_path}")

        # if s3_bucket and s3_prefix:
        #     upload_to_s3(save_path, s3_bucket, s3_prefix)
        #     shutil.rmtree(save_path)  # clean up local copy

        else:
            return False, f"Invalid source type: {source_type}"

        return True, f"Dataset downloaded successfully to {save_path}"
    except Exception as e:
        return False, str(e)
    



def preprocess_data(dataset_path, config):
    df = pd.read_csv(dataset_path)

    # 1. Drop columns
    if "drop_columns" in config:
        df = df.drop(columns=config["drop_columns"], errors="ignore")

    # 2. Drop nulls
    if config.get("drop_null", False):
        df = df.dropna()
    else:
        # 3. Impute missing values
        impute = config.get("impute", {})
        for col, method in impute.items():
            if col in df.columns:
                if method == "mean":
                    df[col] = df[col].fillna(df[col].mean())
                elif method == "median":
                    df[col] = df[col].fillna(df[col].median())
                elif method == "mode":
                    df[col] = df[col].fillna(df[col].mode()[0])

    # 4. Handle outliers
    outliers = config.get("outliers", {})
    for col, method in outliers.items():
        if col in df.columns:
            if method == "clip":
                lower = df[col].quantile(0.01)
                upper = df[col].quantile(0.99)
                df[col] = df[col].clip(lower, upper)
            elif method == "iqr":
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR
                df = df[(df[col] >= lower) & (df[col] <= upper)]

    # 5. Drop duplicates
    if config.get("drop_duplicates", False):
        df = df.drop_duplicates()

    # 6. One-hot encode categorical features
    categorical = config.get("categorical", [])
    df = pd.get_dummies(df, columns=categorical, drop_first=False)

    # 7. Scale numerical features
    scale = config.get("scale", {})
    if scale.get("method") == "standard":
        for col in scale.get("columns", []):
            if col in df.columns:
                mean = df[col].mean()
                std = df[col].std()
                if std != 0:
                    df[col] = (df[col] - mean) / std
                else: df[col] = 0

    # 8. Target column as last column
    target = config.get("target_column")
    if target and target in df.columns:
        df[target] = df.pop(target)

    return df


def collect_csv_metadata(file_path):
    file_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2)

    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        column_names = header
        column_count = len(header)

        # Count remaining rows efficiently
        row_count = sum(1 for _ in reader)

    metadata = {
        "n_rows": row_count,
        "n_cols": column_count,
        "size_mb": file_size_mb,
    }

    return metadata