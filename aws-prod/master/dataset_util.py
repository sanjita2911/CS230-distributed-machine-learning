from datasets import load_dataset
import requests
import kaggle
import os


def download_dataset(url, source_type, save_path):
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

        return True, f"Dataset downloaded successfully to {save_path}"
    except Exception as e:
        return False, str(e)