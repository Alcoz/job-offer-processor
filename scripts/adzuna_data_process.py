from dotenv import load_dotenv
import os
import json

from datetime import datetime
from data_eng.s3_connector import connect_to_s3, send_file_to_s3
from data_eng.adzuna_engine import adzuna_job_offers_getter, adzuna_job_offers_cleaner
import polars as pl

load_dotenv()


adzuna_api_id = os.getenv("ADZUNA_API_ID")
adzuna_api_key = os.getenv("ADZUNA_API_KEY")

minio_api = os.getenv("MINIO_API")

minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_access_key = os.getenv("MINIO_SECRET_ACCESS_KEY")

date = datetime.today().strftime("%Y-%m-%d")

s3_client = connect_to_s3(
    endpoint_url=minio_api,
    access_key_id=minio_access_key,
    secret_access_key=minio_secret_access_key,
    region_name="us-east-1",
)

adzuna_job_list = adzuna_job_offers_getter(adzuna_api_id, adzuna_api_key)

filepath = f"data/adzuna-{date}.json"

with open(filepath, "w") as adzuna_json:
    adzuna_json.write(json.dumps(adzuna_job_list))

send_file_to_s3(
    s3_client=s3_client,
    bucket="job-offers-bucket",
    filepath=filepath,
    s3_filepath=f"adzuna-job-offers-{date}.json",
)

adzuna_job_list = adzuna_job_offers_cleaner(adzuna_job_list)
adzuna_job_list_df = pl.DataFrame(adzuna_job_list)

print(adzuna_job_list_df.head())
