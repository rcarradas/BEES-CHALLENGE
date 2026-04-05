"""
bees_challenge_dag.py
======================
DAG — Open Brewery DB Medallion Pipeline (Bronze → Silver → Gold).

Task graph::

    start → s3_bronze → s3_silver → s3_gold → end

Each task passes its S3 output URI to the next via XCom, so every
operator knows the exact file location produced by its upstream task.

Schedule:
    ``None`` — triggered manually or via external trigger.
    Change to ``"@daily"`` for automated daily ingestion.
"""

__doc__md = """
                bees_challenge_dag.py
                ======================
                DAG — Open Brewery DB Medallion Pipeline (Bronze → Silver → Gold).

                This DAG implements a full medallion architecture pipeline to ingest,
                transform, and aggregate brewery data from the Open Brewery DB API
                into a MinIO-backed data lake.

                Pipeline:
                    start → s3_bronze → s3_silver → s3_gold → end

                Layers:
                    **Bronze** (``s3_bronze``):
                        Fetches all brewery records from the Open Brewery DB API via
                        paginated requests. Persists raw data as Parquet to S3 with no
                        transformations. Partitioned by ``ingestion_date``. Output URI
                        is pushed to XCom for the silver task.

                    **Silver** (``s3_silver``):
                        Reads the exact bronze file produced upstream via XCom URI.
                        Applies type casting, string normalisation, null handling,
                        deduplication, and invalid brewery type coercion. Writes
                        clean Parquet partitioned by ``country`` and ``state_province``.
                        Output base URI is pushed to XCom.

                    **Gold** (``s3_gold``):
                        Reads all silver partitions for the logical date, unions them,
                        and produces a single aggregated Parquet file with the count of
                        breweries per ``country``, ``state_province``, and
                        ``brewery_type``. Validates that no rows were lost during
                        aggregation.

                XCom flow:
                    ``s3_bronze`` → returns ``s3://bronze-layer/.../file.parquet``
                    ``s3_silver`` → consumes bronze URI via Jinja template; returns silver base URI
                    ``s3_gold``   → reads all silver partitions for the logical date

                Schedule:
                    ``None`` — triggered manually. Change to ``"@daily"`` for automated
                    daily ingestion.

                Connections required:
                    - ``minio_conn``: MinIO credentials (``aws_access_key_id``,
                    ``aws_secret_access_key``, ``endpoint_url``) in the Connection
                    ``extra`` JSON field.

                Tags:
                    breweries, medallion, bronze, silver, gold
            """

from datetime import datetime, timedelta

from airflow.models import DAG
from  airflow.providers.standard.operators.empty.EmptyOperator

from plugins.operators.s3_bronze_operator import S3BronzeOperator
from plugins.operators.s3_silver_operator import S3SilverOperator
from plugins.operators.s3_gold_operator import S3GoldOperator

import logging
logger = logging.getLogger(__name__)

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry":   False,
}

with DAG(
    dag_id="bees_challenge_dag",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args=default_args,
    tags=["breweries", "medallion", "bronze", "silver", "gold"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    s3_bronze = S3BronzeOperator(
        task_id="s3_bronze",
    )

    s3_silver = S3SilverOperator(
        task_id="s3_silver",
        bronze_file_uri="{{ task_instance.xcom_pull(task_ids='s3_bronze') }}",
    )

    s3_gold = S3GoldOperator(
        task_id="s3_gold",
    )

    end = EmptyOperator(task_id="end")

    start >> s3_bronze >> s3_silver >> s3_gold >> end