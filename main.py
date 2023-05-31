import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import pyarrow as pa
from datetime import datetime, timedelta
from dateutil import rrule
import xml.etree.ElementTree as ET
from google.cloud import storage
import functions_framework
import gcsfs

bucket_name = "prediswiss-parquet-data-daily"
bucket_data = "prediswiss-parquet-data"

@functions_framework.cloud_event
def to_parquet_daily(cloud_event):
    storage_client = storage.Client(project="prediswiss")
    fs_gcs = gcsfs.GCSFileSystem(project='prediswiss')

    now = datetime.now()
    year_month = now.strftime("%Y-%m")

    parquet_files = fs_gcs.glob(f"{bucket_data}/{year_month}.parquet/*.parquet")

    sorted_files = sorted(parquet_files, key=lambda x: fs_gcs.info(x)["updated"], reverse=True)

    last_24_files = sorted_files[:24]

    data = []
    for file_path in last_24_files:
        table = pq.read_table(file_path, filesystem=fs_gcs)
        df = table.to_pandas()
        data.append(df)

    
    combined_df = pd.concat(data, ignore_index=True)

    table = pa.Table.from_pandas(combined_df)
    datasetPath = now.strftime("%Y")
    path = "gs://" + bucket_name + "/" + datasetPath + ".parquet"
    pq.write_to_dataset(table, root_path=path, filesystem=fs_gcs)

if __name__ == "__main__":
    to_parquet_daily("")

def create_bucket(name, client: storage.Client):    
    bucket = client.create_bucket(name, location="us-east1")
    print(f"Bucket {name} created")
    return bucket

def create_blob(root_bucket: storage.Bucket, destination_name, data_type, data):
    blob = root_bucket.blob(destination_name)
    generation_match_precondition = 0
    blob.upload_from_string(data, data_type, if_generation_match=generation_match_precondition)
    print("file created")

class DataException(Exception):
    "Raised when bucket of data doesn't exist"
    pass