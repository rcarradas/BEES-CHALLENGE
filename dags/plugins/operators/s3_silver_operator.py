from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from typing import List, Dict, Any
from plugins.utils.constants import (
    SILVER_SCHEMA_MAPPING, 
    NOT_NULL_COLUMNS, 
    VALID_BREWERY_TYPES
)
from plugins.utils.aws_s3_utils import S3Handler
import pandas as pd
import logging
import io
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class S3SilverOperator(BaseOperator):

    template_fields = (
        "bronze_file_uri",
        "bronze_layer_bucket", 
        "bronze_layer_prefix", 
        "silver_layer_bucket", 
        "silver_layer_prefix"
    )

    # Silver
    ui_color   = "#C0C0C0"  # silver
    ui_fgcolor = "#000000"

    def __init__(
            self, 
            bronze_file_uri : str, # Example: s3://bronze-layer/openbreweries/breweries/ingestion_date=2026-04-03/20260403T184237Z.parquet
            bronze_layer_bucket : str = "bronze-layer", 
            bronze_layer_prefix : str = "openbreweries/breweries", 
            silver_layer_bucket : str = "silver-layer", 
            silver_layer_prefix : str = "openbreweries/breweries", 
            *args, 
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.bronze_file_uri = bronze_file_uri
        self.bronze_layer_bucket = bronze_layer_bucket
        self.bronze_layer_prefix = bronze_layer_prefix
        self.silver_layer_bucket = silver_layer_bucket
        self.silver_layer_prefix = silver_layer_prefix

    def execute(self, context) -> None:
        s3_handler = S3Handler()

        # 1 - Validating File
        if not self._validate_latest_ingested_file(s3_handler=s3_handler):
            raise AirflowException(f"File {self.bronze_file_uri} does not exist in S3")
        
        # 2 - Reading File
        
        df = self._read_parquet_from_s3(s3_handler=s3_handler)
        logger.info(f'Bronze Shape: {df.shape}')

        # 3 - Transformations

        df = self._cast_types(df=df)

        df = self._normalize_string_columns(df=df)

        df = self._handle_nulls(df=df)

        df = self._deduplicate(df=df)

        df = self._audit_columns(df=df, context=context)

        logger.info(f'Silver Shape: {df.shape}')

        # 4 - Data Quality Checks
        self._data_quality_checks(df=df)


        print(df.head())
        
        


    def _validate_latest_ingested_file(self, s3_handler : S3Handler) -> bool:
        """
        Validates if the latest ingested file exists in S3.
        :param s3_handler: An instance of S3Handler.
        :return: True if the file exists, False otherwise.
        """
        obj_path = self.bronze_file_uri.replace("s3://", "").split("/", 1)
        key = obj_path[1]

        try:
            response = s3_handler.s3.head_object(Bucket=self.bronze_layer_bucket, Key=key)
            return True
        except Exception as err:
            logger.error(f"Failed to get file {self.bronze_file_uri} from S3: {err}")
            return False
        
    def _read_parquet_from_s3(self, s3_handler: S3Handler) -> pd.DataFrame:
        logger.info(f"Reading file {self.bronze_file_uri} from S3")

        s3_obj = s3_handler.s3.get_object(
            Bucket=self.bronze_layer_bucket, 
            Key=self.bronze_file_uri.replace("s3://", "").split("/", 1)[1]
        )
        file_buffer = io.BytesIO(s3_obj["Body"].read())

        return pd.read_parquet(file_buffer, engine="pyarrow")
    
    def _cast_types(self, df : pd.DataFrame) -> pd.DataFrame:
        schema_columns = set(SILVER_SCHEMA_MAPPING.keys())
        api_columns = set(df.columns) - {"_ingested_at", "_source", "_dag_id", "_dag_run_id", "_logical_date"}
        unknown_cols  = api_columns - schema_columns
        missing_cols  = schema_columns - api_columns
 
        logger.info(f'Casting columns {api_columns} to types {SILVER_SCHEMA_MAPPING.values()}')

        if unknown_cols:
            logger.warning(f"Unknown columns in source (kept as-is): {unknown_cols}")
        if missing_cols:
            # Add missing columns as null
            logger.warning(f"Missing columns in source (added as null): {missing_cols}")
            for col in missing_cols:
                df[col] = None

        for col, col_type in SILVER_SCHEMA_MAPPING.items():
            try:
                if col_type == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(col_type)
            except Exception as err:
                logger.error(f"Failed to cast column {col} to type {col_type}: {err}")

        return df
    
    def _normalize_string_columns(self, df : pd.DataFrame) -> pd.DataFrame:
        
        string_columns = [column for column in SILVER_SCHEMA_MAPPING.keys() if SILVER_SCHEMA_MAPPING[column] == "string"]

        logger.info(f'Normalizing columns {string_columns}')

        for s in string_columns:
            if s in df.columns:
                df[s] = df[s].str.lower().str.strip().replace("", None)

        if "brewery_type" in df.columns:
            df["brewery_type"] = df["brewery_type"].str.lower().str.strip()

            # Dealing with invalid values catched in _data_quality_checks during development
            # Exception: Unexpected brewery_type values: {'location', 'taproom', 'beergarden', 'cidery'}
            df["brewery_type"] = pd.Categorical(df["brewery_type"], categories=VALID_BREWERY_TYPES)

        return df
    
    def _handle_nulls(self, df : pd.DataFrame) -> pd.DataFrame:
        
        logger.info("Dropping rows with null values")

        length = len(df)
        df = df.dropna(subset=NOT_NULL_COLUMNS)

        if len(df) < length:
            logger.warning(f"Dropped {length - len(df)} rows with null values")

        return df
    
    def _deduplicate(self, df : pd.DataFrame) -> pd.DataFrame:

        logger.info("Dropping duplicate rows")

        length = len(df)
        df =  df.drop_duplicates(subset=["id"], keep="last")

        if len(df) < length:
            logger.warning(f"Dropped {length - len(df)} duplicate rows")

        return df.reset_index(drop=True)
    
    def _audit_columns(self, df : pd.DataFrame, context : dict) -> pd.DataFrame:

        logger.info("Adding audit columns")

        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        df["_silver_dag_run_id"]   = context["run_id"]
        return df
    
    def _data_quality_checks(self, df : pd.DataFrame) -> None:

        logger.info("Running data quality checks")

        errors = []

        if df.empty:
            logger.info("Dataframe is empty")
            errors.append("Dataframe is empty")

        if df.id.duplicated().any():
            logger.warning("Duplicate IDs found in dataframe")
            errors.append("Duplicate IDs found in dataframe")

        if "brewery_type" in df.columns:
            invalid_types = set(df["brewery_type"].dropna().unique()) - VALID_BREWERY_TYPES
            if invalid_types:
                errors.append(f"Unexpected brewery_type values: {invalid_types}")

        if errors:
            for error in errors:
                logger.error(error)
            raise AirflowException("\n".join(errors))

        logger.info(f"Data quality checks passed for dataframe with shape {df['brewery_type'].unique()}")