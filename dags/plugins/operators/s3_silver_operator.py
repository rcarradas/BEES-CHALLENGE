"""
s3_silver_operator.py
======================
Airflow Operator for transforming bronze brewery data into the silver layer.

The silver layer applies the following transformations to the raw bronze data:

1. **Type casting** — columns are cast to the types defined in
   ``SILVER_SCHEMA_MAPPING``. Coordinates use ``pd.to_numeric`` with
   ``errors='coerce'`` so malformed values become ``NaN`` rather than
   raising.

2. **String normalisation** — leading/trailing whitespace is stripped
   from all string columns; empty strings are replaced with ``None``.
   ``brewery_type`` is additionally lowercased for consistent
   categorisation. Other string columns (names, addresses) are *not*
   lowercased to preserve display casing.

3. **Null handling** — rows with null values in ``NOT_NULL_COLUMNS``
   (identity fields: ``id``, ``name``, ``brewery_type``, ``country``)
   are dropped. Nulls in optional columns (coordinates, phone, etc.)
   are retained.

4. **Deduplication** — duplicate ``id`` values are removed, keeping
   the last occurrence to favour the most recent record in case of
   re-ingestion.

5. **Invalid brewery type handling** — values outside ``VALID_BREWERY_TYPES``
   (e.g. ``taproom``, ``cidery``, observed in production data) are
   converted to ``NaN`` by casting to ``pd.Categorical`` with an explicit
   category list.

6. **Partitioned write** — output is written as Parquet partitioned by
   ``country`` and ``state_province`` using Hive conventions, enabling
   partition pruning in Athena and Spark for location-based queries.

Example:
    DAG usage::

        from plugins.operators.s3_silver_operator import S3SilverOperator

        transform = S3SilverOperator(
            task_id="transform_breweries_to_silver",
            bronze_file_uri="{{ task_instance.xcom_pull(task_ids='s3_bronze') }}",
        )
"""

import io
import logging
from datetime import datetime, timezone
from typing import List
import unicodedata
import re

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from plugins.utils.aws_s3_utils import S3Handler
from plugins.utils.constants import (
    NOT_NULL_COLUMNS,
    SILVER_SCHEMA_MAPPING,
    VALID_BREWERY_TYPES,
)

logger = logging.getLogger(__name__)


class S3SilverOperator(BaseOperator):
    """Transform raw bronze Parquet into a cleaned, partitioned silver layer.

    Reads the exact bronze file produced by the upstream
    ``S3BronzeOperator`` (via XCom URI), applies all silver
    transformations, validates data quality, and writes partitioned
    Parquet files to the silver S3 bucket.

    Attributes:
        ui_color (str): Silver hex colour shown in the Airflow Graph view.
        ui_fgcolor (str): Foreground (text) colour for the task box.
        template_fields (tuple): Fields that support Jinja templating.

    Args:
        bronze_file_uri (str): Full S3 URI of the bronze Parquet file to
            process. Typically populated via XCom Jinja template:
            ``"{{ task_instance.xcom_pull(task_ids='s3_bronze') }}"``.
        bronze_layer_bucket (str): Bucket containing the bronze file.
            Defaults to ``"bronze-layer"``.
        bronze_layer_prefix (str): Key prefix of the bronze dataset.
            Defaults to ``"openbreweries/breweries"``.
        silver_layer_bucket (str): Target bucket for silver output.
            Defaults to ``"silver-layer"``.
        silver_layer_prefix (str): Key prefix for silver output files.
            Defaults to ``"openbreweries/breweries"``.
        **kwargs: Passed through to ``BaseOperator.__init__``.
    """

    template_fields = (
        "bronze_file_uri",
        "bronze_layer_bucket",
        "bronze_layer_prefix",
        "silver_layer_bucket",
        "silver_layer_prefix",
    )
    ui_color = "#C0C0C0"   # silver
    ui_fgcolor = "#000000"

    def __init__(
        self,
        bronze_file_uri: str,
        bronze_layer_bucket: str = "bronze-layer",
        bronze_layer_prefix: str = "openbreweries/breweries",
        silver_layer_bucket: str = "silver-layer",
        silver_layer_prefix: str = "openbreweries/breweries",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.bronze_file_uri = bronze_file_uri
        self.bronze_layer_bucket = bronze_layer_bucket
        self.bronze_layer_prefix = bronze_layer_prefix
        self.silver_layer_bucket = silver_layer_bucket
        self.silver_layer_prefix = silver_layer_prefix

    def execute(self, context: dict) -> str:
        """Run the silver transformation pipeline.

        Steps:

        1. Validate that the bronze file exists in S3.
        2. Read the Parquet file into a DataFrame.
        3. Apply type casting, string normalisation, null handling,
           deduplication, and audit column attachment.
        4. Run data quality checks — raises on failure.
        5. Write partitioned Parquet to the silver S3 bucket.

        Args:
            context (dict): Airflow task context.

        Returns:
            str: Base S3 URI of the silver output prefix
                (e.g. ``s3://silver-layer/openbreweries/breweries``).
                Automatically pushed to XCom.

        Raises:
            AirflowException: If the bronze file does not exist, or if
                any data quality check fails.
        """
        s3_handler = S3Handler()

        # 1 — Validate source file
        if not self._validate_latest_ingested_file(s3_handler=s3_handler):
            raise AirflowException(
                f"Bronze file not found: {self.bronze_file_uri}. "
                "Ensure the bronze task completed successfully."
            )

        # 2 — Read
        df = self._read_parquet_from_s3(s3_handler=s3_handler)
        logger.info("Bronze shape: %s", df.shape)

        # 3 — Transform
        df = self._cast_types(df=df)
        df = self._normalize_string_columns(df=df)
        df = self._handle_nulls(df=df)
        df = self._deduplicate(df=df)
        df = self._audit_columns(df=df, context=context)
        logger.info("Silver shape after transforms: %s", df.shape)

        # 4 — Validate
        self._data_quality_checks(df=df)

        # 5 — Write
        logical_date = str(context["logical_date"])[:10]  # "YYYY-MM-DD"
        silver_uri = self._write_silver(
            s3_hook=s3_handler, df=df, logical_date=logical_date
        )
        return silver_uri

    # ------------------------------------------------------------------
    # Step 1 — Validation
    # ------------------------------------------------------------------

    def _validate_latest_ingested_file(self, s3_handler: S3Handler) -> bool:
        """Check that the bronze Parquet file exists in S3 before reading.

        Uses ``head_object`` (a lightweight metadata call) rather than
        attempting a full download, avoiding unnecessary data transfer on
        failure.

        Args:
            s3_handler (S3Handler): Configured S3 client wrapper.

        Returns:
            bool: ``True`` if the object exists, ``False`` otherwise.
        """
        key = self.bronze_file_uri.replace("s3://", "").split("/", 1)[1]
        try:
            s3_handler.s3.head_object(Bucket=self.bronze_layer_bucket, Key=key)
            logger.info("Validated bronze file: %s", self.bronze_file_uri)
            return True
        except Exception as err:
            logger.error("Failed to locate bronze file %s: %s", self.bronze_file_uri, err)
            return False

    # ------------------------------------------------------------------
    # Step 2 — Read
    # ------------------------------------------------------------------

    def _read_parquet_from_s3(self, s3_handler: S3Handler) -> pd.DataFrame:
        """Download and deserialise the bronze Parquet file from S3.

        Uses an in-memory ``BytesIO`` buffer to avoid writing temp files
        to the worker's local filesystem, which may be ephemeral in
        containerised environments (Kubernetes, ECS).

        Args:
            s3_handler (S3Handler): Configured S3 client wrapper.

        Returns:
            pd.DataFrame: Raw bronze DataFrame as stored on S3.

        Raises:
            botocore.exceptions.ClientError: If the object cannot be read.
        """
        logger.info("Reading bronze file: %s", self.bronze_file_uri)
        key = self.bronze_file_uri.replace("s3://", "").split("/", 1)[1]
        s3_obj = s3_handler.s3.get_object(Bucket=self.bronze_layer_bucket, Key=key)
        return pd.read_parquet(io.BytesIO(s3_obj["Body"].read()), engine="pyarrow")

    # ------------------------------------------------------------------
    # Step 3 — Transformations
    # ------------------------------------------------------------------

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply the silver schema contract to the DataFrame columns.

        Columns present in the source but absent from ``SILVER_SCHEMA_MAPPING``
        are retained as-is (logged as warnings). Columns defined in the
        schema but absent from the source are added as ``None`` columns
        for forward-compatibility with API additions.

        ``float64`` columns (``latitude``, ``longitude``) use
        ``pd.to_numeric`` with ``errors='coerce'`` so malformed coordinate
        strings become ``NaN`` rather than raising a cast exception.

        Args:
            df (pd.DataFrame): Input DataFrame from the bronze layer.

        Returns:
            pd.DataFrame: DataFrame with columns cast to silver schema types.
        """
        schema_columns = set(SILVER_SCHEMA_MAPPING.keys())
        audit_columns = {"_ingested_at", "_source", "_dag_id", "_dag_run_id", "_logical_date"}
        api_columns = set(df.columns) - audit_columns

        unknown_cols = api_columns - schema_columns
        missing_cols = schema_columns - api_columns

        if unknown_cols:
            logger.warning("Unknown source columns (kept as-is): %s", unknown_cols)
        if missing_cols:
            logger.warning("Missing schema columns (added as null): %s", missing_cols)
            for col in missing_cols:
                df[col] = None

        for col, col_type in SILVER_SCHEMA_MAPPING.items():
            try:
                if col_type == "float64":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                else:
                    df[col] = df[col].astype(col_type)
            except Exception as err:
                logger.error("Failed to cast column '%s' to %s: %s", col, col_type, err)

        return df

    def _normalize_string_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalise string columns and enforce valid brewery type categories.

        For all string columns:
            - Strips leading/trailing whitespace.
            - Replaces empty strings with ``None`` (null in Parquet).
            - Does **not** lowercase names or addresses — display casing
              is preserved for BI and reporting consumers.

        For ``brewery_type`` specifically:
            - Lowercased for consistent categorical matching.
            - Cast to ``pd.Categorical`` with ``categories=VALID_BREWERY_TYPES``.
              Values outside the valid set (e.g. ``taproom``, ``cidery``)
              become ``NaN``. This must happen **before** ``_data_quality_checks``
              because the quality check validates against ``VALID_BREWERY_TYPES``;
              after the categorical cast, all remaining non-null values are
              guaranteed to be valid.

        Args:
            df (pd.DataFrame): Type-cast DataFrame.

        Returns:
            pd.DataFrame: DataFrame with normalised string columns.
        """
        string_columns = [
            col for col, dtype in SILVER_SCHEMA_MAPPING.items() if dtype == "string"
        ]
        logger.info("Normalising string columns: %s", string_columns)

        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].str.strip().replace("", None)

        if "brewery_type" in df.columns:
            df["brewery_type"] = df["brewery_type"].str.lower().str.strip()
            df["brewery_type"] = pd.Categorical(
                df["brewery_type"], categories=VALID_BREWERY_TYPES
            )

        return df

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows with nulls in identity columns.

        Rows missing any ``NOT_NULL_COLUMNS`` value are dropped because
        they cannot be meaningfully used in downstream analysis or joins.
        All other null values (optional fields) are retained.

        Args:
            df (pd.DataFrame): Normalised DataFrame.

        Returns:
            pd.DataFrame: DataFrame with incomplete identity rows removed.
        """
        length = len(df)
        df = df.dropna(subset=NOT_NULL_COLUMNS)
        dropped = length - len(df)
        if dropped:
            logger.warning(
                "Dropped %d rows with nulls in required columns %s",
                dropped, NOT_NULL_COLUMNS,
            )
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records by business key ``id``.

        Keeps the last occurrence to favour the most recent record in
        cases where the same brewery ``id`` appears across multiple
        bronze ingestion runs.

        Args:
            df (pd.DataFrame): Null-handled DataFrame.

        Returns:
            pd.DataFrame: Deduplicated DataFrame with reset index.
        """
        length = len(df)
        df = df.drop_duplicates(subset=["id"], keep="last")
        dupes = length - len(df)
        if dupes:
            logger.warning("Removed %d duplicate rows by 'id'", dupes)
        return df.reset_index(drop=True)

    def _audit_columns(self, df: pd.DataFrame, context: dict) -> pd.DataFrame:
        """Attach silver-layer audit columns to the DataFrame.

        Adds processing timestamp and DAG run ID for lineage tracking
        in addition to the bronze audit columns already present.

        Args:
            df (pd.DataFrame): Transformed DataFrame.
            context (dict): Airflow task context.

        Returns:
            pd.DataFrame: DataFrame with silver audit columns appended.
        """
        df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()
        df["_silver_dag_run_id"] = context["run_id"]
        return df

    # ------------------------------------------------------------------
    # Step 4 — Data Quality Checks
    # ------------------------------------------------------------------

    def _data_quality_checks(self, df: pd.DataFrame) -> None:
        """Assert data quality rules before writing to silver.

        Checks are collected and raised together as a single
        ``AirflowException`` so all failures are visible at once.

        Rules:
            - DataFrame must not be empty.
            - ``id`` column must have no duplicates.

        Note:
            ``brewery_type`` validity is **not** checked here because
            ``_normalize_string_columns`` already coerces invalid types
            to ``NaN`` via ``pd.Categorical``. All non-null ``brewery_type``
            values are guaranteed valid at this point.

        Args:
            df (pd.DataFrame): Fully transformed silver DataFrame.

        Raises:
            AirflowException: If any quality rule fails. The exception
                message lists all failures.
        """
        logger.info("Running data quality checks")
        errors: List[str] = []

        if df.empty:
            errors.append("DataFrame is empty after transformations.")

        if df["id"].duplicated().any():
            errors.append("Duplicate 'id' values found after deduplication step.")

        if errors:
            for error in errors:
                logger.error("Quality check failed: %s", error)
            raise AirflowException(
                f"Silver data quality failed ({len(errors)} error(s)):\n"
                + "\n".join(errors)
            )

        logger.info(
            "Data quality checks passed. brewery_type distribution: %s",
            df["brewery_type"].value_counts().to_dict(),
        )

    # ------------------------------------------------------------------
    # Step 5 — Sanitise Partition Value
    # ------------------------------------------------------------------

    def _sanitize_partition_value(self, value: str) -> str:
        """Normalise a partition key value for safe use in S3 paths.

        Steps:
            1. Normalize unicode (NFD) then encode to ASCII, dropping characters
            that have no ASCII equivalent (handles ö → o, ä → a, å → a).
            2. Replace spaces and hyphens with underscores.
            3. Strip any remaining non-alphanumeric characters except underscores.
            4. Lowercase for consistency.

        Args:
            value (str): Raw partition value from the DataFrame.

        Returns:
            str: S3-safe ASCII string.
        """
        # Step 1 — decompose unicode characters and drop the accent marks
        normalized = (
            unicodedata.normalize("NFD", value)
            .encode("ascii", errors="ignore")
            .decode("ascii")
        )
        # Step 2 — spaces and hyphens to underscores
        normalized = re.sub(r"[\s\-]+", "_", normalized)
        # Step 3 — strip anything that isn't alphanumeric or underscore
        normalized = re.sub(r"[^\w]", "", normalized)
        # Step 4 — lowercase
        return normalized.lower()
    
    # ------------------------------------------------------------------
    # Step 5.1 — Write
    # ------------------------------------------------------------------

    def _write_silver(
        self,
        s3_hook: S3Handler,
        df: pd.DataFrame,
        logical_date: str,
    ) -> str:
        """Write the silver DataFrame as partitioned Parquet files to S3.

        Partitions the data by ``country`` and ``state_province`` using
        Hive-style directory naming (``country=X/state_province=Y/``).
        This allows query engines like Athena and Spark to prune partitions
        when filtering by location, avoiding full dataset scans.

        Partition columns are dropped from the Parquet file content
        because they are already encoded in the S3 path; query engines
        inject them back as virtual columns automatically.

        ``fillna("unknown")`` is applied to partition columns only,
        ensuring every row is written (no rows silently skipped by
        ``groupby``) while avoiding S3 key parsing issues caused by
        ``None``-named directories.

        ``replace=True`` is intentional — silver is a reproducible,
        reprocessable layer. Re-running the DAG for a past date should
        overwrite the previous output with corrected data.

        Args:
            s3_hook (S3Handler): Configured S3 client wrapper.
            df (pd.DataFrame): Fully validated silver DataFrame.
            logical_date (str): DAG logical date in ``YYYY-MM-DD`` format,
                used as the Parquet filename.

        Returns:
            str: Base S3 URI of the silver prefix
                (e.g. ``s3://silver-layer/openbreweries/breweries``).
        """
        partition_cols = ["country", "state_province"]
        df_write = df.copy()
        df_write["country"] = df_write["country"].fillna("unknown")
        df_write["state_province"] = df_write["state_province"].fillna("unknown")

        written_files: list[str] = []

        for (country, state), group in df_write.groupby(partition_cols, observed=True):
            group = group.drop(columns=partition_cols)

            safe_country = self._sanitize_partition_value(str(country))
            safe_state   = self._sanitize_partition_value(str(state))

            s3_key = (
                f"{self.silver_layer_prefix}"
                f"/country={safe_country}"
                f"/state_province={safe_state}"
                f"/{logical_date}.parquet"
            )

            buffer = io.BytesIO()
            group.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)

            s3_hook.s3.upload_fileobj(
                Fileobj=buffer,
                Bucket=self.silver_layer_bucket,
                Key=s3_key,
            )

            written_files.append(s3_key)
            logger.info("Written: %s (%d rows)", s3_key, len(group))

        logger.info("Total partitions written: %d", len(written_files))
        return f"s3://{self.silver_layer_bucket}/{self.silver_layer_prefix}"