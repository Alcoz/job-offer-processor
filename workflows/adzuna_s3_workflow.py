from prefect import flow, get_run_logger
from prefect.assets import materialize

from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os
import json
from prefect_aws.s3 import S3Bucket

from data_eng.adzuna_engine import adzuna_job_offers_getter


@materialize("s3://job-offers-bucket/data/adzuna-job-offers-latest.json")
def adzuna_get_data() -> dict:
    logger = get_run_logger()
    logger.info("🚀 Starting process_data")

    date = datetime.today().strftime("%Y-%m-%d")
    filepath = f"data/adzuna-{date}.json"
    logger.info(f"📁 Filepath set to: {filepath}")

    load_dotenv()
    logger.info("🔐 Environment variables loaded")

    adzuna_api_id = os.getenv("ADZUNA_API_ID")
    adzuna_api_key = os.getenv("ADZUNA_API_KEY")

    s3_bucket_block = S3Bucket.load("s3-block")

    if not adzuna_api_id or not adzuna_api_key:
        logger.error("❌ Missing Adzuna API credentials")
        raise ValueError("Missing Adzuna API credentials")

    logger.info("✅ Adzuna credentials found")

    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_ACCESS_KEY")

    logger.info("☁️ MinIO/S3 environment configured")

    # Step 1: Fetch data
    logger.info("📡 Fetching job offers from Adzuna...")
    adzuna_job_list = adzuna_job_offers_getter(adzuna_api_id, adzuna_api_key)
    logger.info(f"📊 Raw jobs fetched: {len(adzuna_job_list)}")

    # Step 2: Save locally
    logger.info("💾 Saving data locally...")
    os.makedirs("data", exist_ok=True)

    with open(filepath, "w") as adzuna_json:
        json.dump(adzuna_job_list, adzuna_json)

    logger.info("✅ Local file saved")

    s3_bucket_block.upload_from_path(
        from_path=filepath, to_path=f"data/adzuna-job-offers-{date}.json"
    )

    s3_bucket_block.upload_from_path(
        from_path=filepath, to_path="data/adzuna-job-offers-latest.json"
    )

    # Important debug: show sample
    logger.info(f"🔍 Sample job: {adzuna_job_list[0] if adzuna_job_list else 'EMPTY'}")

    return adzuna_job_list


@flow(log_prints=True)
def data_pipeline():
    logger = get_run_logger()
    logger.info("🌊 Starting data pipeline")

    adzuna_data = adzuna_get_data()

    logger.info("🎉 Pipeline finished")
    logger.info(f"📦 Final dataset size: {len(adzuna_data)}")


if __name__ == "__main__":
    data_pipeline.from_source(
        source=str(Path(__file__).parent),  # code stored in local directory
        entrypoint="adzuna_s3_workflow.py:data_pipeline",
    ).deploy(
        name="adzuna_flow",
        work_pool_name="default-agent-pool",
        cron="0 3 * * *",
        push=False,
    )
