from airflow.models import BaseOperator
from typing import List, Dict, Any
from airflow.hooks.base import BaseHook
from plugins.hooks.open_brewery_hook import OpenBreweryHook
from plugins.utils.constants import (
    BREWERIES_API_URL,
    MAX_RETRIES,
    MAX_REGISTRIES_PER_PAGE,
    EXPONENTIAL_BACKOFF_SECONDS,
)
import io
import boto3
from typing import Optional
import pandas as pd
from datetime import datetime, timezone
import logging
logger = logging.getLogger(__name__)  



class S3BronzeOperator(BaseOperator):

    template_fields = ("s3_bucket", "s3_prefix")
    # Bronze
    ui_color   = "#CD7F32"  # bronze
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
            **kwargs
        ) -> None:
        super().__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_conn_id = aws_conn_id
        self.openbrewery_url = openbrewery_url
        self.max_retries = max_retries
        self.register_per_page = register_per_page
        self.exponential_backoff_seconds = exponential_backoff_seconds

    def execute(self, context) -> None:
        s3_hook = self._get_minio_base_hook(conn_id=self.aws_conn_id) #S3Hook(aws_conn_id=self.aws_conn_id)
        open_brewery_hook = OpenBreweryHook(
            url=self.openbrewery_url,
            max_retries=self.max_retries,
            register_per_page=self.register_per_page,
            exponential_backoff_seconds=self.exponential_backoff_seconds
        )

        # 1 - Extract data from Open Brewery DB API

        logger.info(f'Starting to fetch breweries from Open Brewery DB API and upload to S3 bucket {self.s3_bucket} with prefix /{self.s3_prefix}')

        breweries = open_brewery_hook.get_breweries()

        logger.info(f'Fetched {len(breweries)} breweries from Open Brewery DB API')

        if not breweries:
            raise ValueError("No breweries found in Open Brewery DB API")
        
        # 2 - Convert to dataframe
        df = self._convert_to_daframe(breweries, context)

        logger.info(f'Datafram head {df.head()}')

        # 3 - Serialzie to parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # 4 - Upload to S3
        s3_key = self._build_s3_key()

        s3_hook.upload_fileobj(
            Fileobj=parquet_buffer,
            Bucket=self.s3_bucket,
            Key=s3_key,
        )

        logger.info(f'Uploaded {len(df)} breweries to S3 bucket {self.s3_bucket} with prefix {self.s3_prefix}')

        return f's3://{self.s3_bucket}/{s3_key}'

    def _convert_to_daframe(self, records: List[Dict[str, Any]], context: dict ) -> pd.DataFrame:
        df = pd.DataFrame(records)

        df['_ingested_at'] = datetime.now(tz=timezone.utc).isoformat()
        df['_source'] = self.openbrewery_url
        df["_dag_id"]          = context["dag"].dag_id
        df["_dag_run_id"]      = context["run_id"]
        df["_logical_date"]    = str(context["logical_date"])

        logger.info(f'Dataframe assembled with {len(df)} rows and columns {df.columns}')

        return df
    
    def _build_s3_key(self) -> str:
        """
        Builds a Hive-partitioned S3 key.
        Pattern: <prefix>/ingestion_date=YYYY-MM-DD/<timestamp>Z.parquet
        """
        now       = datetime.now(timezone.utc)
        date_part = now.strftime("%Y-%m-%d")
        ts_part   = now.strftime("%Y%m%dT%H%M%SZ")
        return f"{self.s3_prefix}/ingestion_date={date_part}/{ts_part}.parquet"
    
    def _get_minio_base_hook(self, conn_id: str) -> boto3.client:
        minio = BaseHook.get_connection(conn_id).extra_dejson

        s3 = boto3.client(
                "s3",
                aws_access_key_id=minio['aws_access_key_id'],
                aws_secret_access_key=minio['aws_secret_access_key'],
                endpoint_url=minio['endpoint_url'],
        )
        return s3