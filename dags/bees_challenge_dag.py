from airflow.models import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from plugins.operators.s3_bronze_operator import S3BronzeOperator
from plugins.operators.s3_silver_operator import S3SilverOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd

import logging
logger = logging.getLogger(__name__)


with DAG(
    dag_id= "bees_challenge_dag",
    start_date=datetime(2022, 1, 1),
    schedule = None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    s3_bronze = S3BronzeOperator(
        task_id="s3_bronze",
    )

    # @task
    # def read_from_minio(**kwargs):
    #     """
    #     Reads a Parquet file from MinIO and returns a Pandas DataFrame.
    #     """
    #     file_uri = kwargs["ti"].xcom_pull(task_ids="s3_bronze")
    #     logger.info(f"File URI: {file_uri}")
    #     minio_options = {
    #         "key": "admin",
    #         "secret": "password",
    #         "client_kwargs": {"endpoint_url": "http://minio:9000"}
    #     }
    #     df = pd.read_parquet(file_uri, storage_options=minio_options)
    #     return df
    
    s3_silver = S3SilverOperator(
        task_id="s3_silver",
        bronze_file_uri="{{ task_instance.xcom_pull(task_ids='s3_bronze', key='return_value') }}",
    )

    end = EmptyOperator(task_id="end")

    #start >> s3_bronze >> end
    #start >> s3_bronze >> read_from_minio() >> end
    start >> s3_bronze >> s3_silver >> end