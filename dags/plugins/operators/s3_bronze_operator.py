"""
s3_bronze_operator.py
======================
Airflow Operator for ingesting raw brewery data into the bronze S3 layer.

The bronze layer stores data exactly as received from the source API —
no transformations, no type coercion. The only additions are audit columns
(``_ingested_at``, ``_source``, ``_dag_id``, ``_dag_run_id``,
``_logical_date``) required for lineage tracking.

Files are written as Parquet and partitioned by ingestion date using the
Hive convention (``ingestion_date=YYYY-MM-DD/``), making them queryable
by date range in Athena or Spark without full scans.

The operator returns the S3 URI of the written file, which is
automatically pushed to XCom so the downstream silver operator can
locate the exact file to process.

Example:
    DAG usage::

        from plugins.operators.s3_bronze_operator import S3BronzeOperator

        ingest = S3BronzeOperator(
            task_id="ingest_breweries_to_bronze",
            s3_bucket="bronze-layer",
            s3_prefix="openbreweries/breweries",
        )
"""

import io
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from airflow.models import BaseOperator

from plugins.hooks.open_brewery_hook import OpenBreweryHook
from plugins.utils.aws_s3_utils import S3Handler
from plugins.utils.constants import (
    BREWERIES_API_URL,
    EXPONENTIAL_BACKOFF_SECONDS,
    MAX_REGISTRIES_PER_PAGE,
    MAX_RETRIES,
)

logger = logging.getLogger(__name__)


class S3BronzeOperator(BaseOperator):
    """Extract all brewery records from the API and persist raw Parquet to S3.

    Follows the bronze layer principle: data lands exactly as received.
    The only mutation is the addition of pipeline audit columns.

    Attributes:
        ui_color (str): Bronze hex colour shown in the Airflow Graph view.
        ui_fgcolor (str): Foreground (text) colour for the task box.
        template_fields (tuple): Fields that support Jinja templating.

    Args:
        s3_bucket (str): Target S3 bucket. Defaults to ``"bronze-layer"``.
        s3_prefix (str): S3 key prefix for output files.
            Defaults to ``"openbreweries/breweries"``.
        aws_conn_id (str): Airflow Connection ID for MinIO/S3 credentials.
            Defaults to ``"minio_conn"``.
        openbrewery_url (str): Full URL of the breweries API endpoint.
        max_retries (int): Maximum HTTP retry attempts per page.
        register_per_page (int): Records requested per paginated API call.
        exponential_backoff_seconds (int): Base back-off interval in seconds.
        **kwargs: Passed through to ``BaseOperator.__init__``.
    """

    template_fields = ("s3_bucket", "s3_prefix")
    ui_color = "#CD7F32"   # bronze
    ui_fgcolor = "#000000"

    def __init__(
        self,
        s3_bucket: Optional[str] = "bronze-layer",
        s3_prefix: Optional[str] = "openbreweries/breweries",
        aws_conn_id: Optional[str] = "minio_conn",
        openbrewery_url: Optional[str] = BREWERIES_API_URL,
        max_retries: Optional[int] = MAX_RETRIES,
        register_per_page: Optional[int] = MAX_REGISTRIES_PER_PAGE,
        exponential_backoff_seconds: Optional[int] = EXPONENTIAL_BACKOFF_SECONDS,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_conn_id = aws_conn_id
        self.openbrewery_url = openbrewery_url
        self.max_retries = max_retries
        self.register_per_page = register_per_page
        self.exponential_backoff_seconds = exponential_backoff_seconds

    def execute(self, context: dict) -> str:
        """Run the bronze ingestion pipeline.

        Steps:

        1. Instantiate ``S3Handler`` and ``OpenBreweryHook``.
        2. Fetch all brewery records via the Hook (pagination handled internally).
        3. Convert records to a ``pandas.DataFrame`` and attach audit columns.
        4. Serialise the DataFrame to Parquet in-memory (no temp files).
        5. Upload the Parquet buffer to S3 under a Hive-partitioned key.

        Args:
            context (dict): Airflow task context, used to populate audit columns.

        Returns:
            str: Full S3 URI of the written file
                (e.g. ``s3://bronze-layer/openbreweries/breweries/ingestion_date=2024-01-01/20240101T030000Z.parquet``).
                This value is automatically pushed to XCom.

        Raises:
            ValueError: If the API returns zero records.
        """
        s3_hook = S3Handler(conn_id=self.aws_conn_id)
        open_brewery_hook = OpenBreweryHook(
            url=self.openbrewery_url,
            max_retries=self.max_retries,
            register_per_page=self.register_per_page,
            exponential_backoff_seconds=self.exponential_backoff_seconds,
        )

        logger.info(
            "Starting extraction — target: s3://%s/%s",
            self.s3_bucket, self.s3_prefix,
        )

        # 1 — Extract
        breweries = open_brewery_hook.get_breweries()
        logger.info("Fetched %d breweries from API", len(breweries))

        if not breweries:
            raise ValueError("API returned zero records — aborting bronze ingestion.")

        # 2 — Build DataFrame (no transformations)
        df = self._convert_to_dataframe(breweries, context)

        # 3 — Serialise to Parquet in-memory
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)

        # 4 — Upload to S3
        s3_key = self._build_s3_key()
        s3_hook.s3.upload_fileobj(
            Fileobj=parquet_buffer,
            Bucket=self.s3_bucket,
            Key=s3_key,
        )

        logger.info("Uploaded %d rows to s3://%s/%s", len(df), self.s3_bucket, s3_key)
        return f"s3://{self.s3_bucket}/{s3_key}"

    def _convert_to_dataframe(
        self,
        records: List[Dict[str, Any]],
        context: dict,
    ) -> pd.DataFrame:
        """Convert raw API records to a DataFrame and attach audit columns.

        No source columns are modified — this is a bronze layer operation.
        Only additive audit columns are appended.

        Args:
            records (List[Dict[str, Any]]): Raw records from the API.
            context (dict): Airflow task context for audit metadata.

        Returns:
            pd.DataFrame: DataFrame with original columns plus audit fields:
                ``_ingested_at``, ``_source``, ``_dag_id``,
                ``_dag_run_id``, ``_logical_date``.
        """
        df = pd.DataFrame(records)

        df["_ingested_at"]  = datetime.now(tz=timezone.utc).isoformat()
        df["_source"]       = self.openbrewery_url
        df["_dag_id"]       = context["dag"].dag_id
        df["_dag_run_id"]   = context["run_id"]
        df["_logical_date"] = str(context["logical_date"])

        logger.info("DataFrame assembled: %d rows, %d columns", len(df), len(df.columns))
        return df

    def _build_s3_key(self) -> str:
        """Build a Hive-partitioned S3 object key for the current run.

        The key pattern encodes the ingestion date as a Hive partition
        directory and uses a UTC timestamp as the filename, ensuring
        multiple runs on the same day produce distinct, non-overwriting
        files.

        Pattern::

            <prefix>/ingestion_date=YYYY-MM-DD/<YYYYMMDDTHHMMSSz>.parquet

        Returns:
            str: S3 object key (without bucket prefix).
        """
        now = datetime.now(timezone.utc)
        date_part = now.strftime("%Y-%m-%d")
        ts_part = now.strftime("%Y%m%dT%H%M%SZ")
        return f"{self.s3_prefix}/ingestion_date={date_part}/{ts_part}.parquet"