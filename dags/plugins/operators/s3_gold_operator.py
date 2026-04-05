"""
s3_gold_operator.py
====================
Airflow Operator for producing the gold aggregation layer.

The gold layer answers a single business question defined by the case:

    "Quantity of breweries per type and location."

It reads all silver Parquet partitions for the DAG's logical date,
unions them into a single DataFrame, aggregates by
``country × state_province × brewery_type``, and writes one compact
Parquet file to the gold S3 bucket.

Design decisions:
    - **Reads from silver, never bronze** — gold trusts that silver is
      already clean, typed, and deduplicated.
    - **Single output file** — the aggregation is small enough that
      partitioning adds no query benefit. BI tools read the whole file.
    - **No new Hook** — both source and destination are S3, so
      ``S3Handler`` covers everything.
    - **``replace=True``** — like silver, gold is reprocessable. Re-running
      the DAG for a past date should overwrite the previous aggregation.

Example:
    DAG usage::

        from plugins.operators.s3_gold_operator import S3GoldOperator

        aggregate = S3GoldOperator(
            task_id="aggregate_breweries_to_gold",
            silver_uri="{{ task_instance.xcom_pull(task_ids='s3_silver') }}",
        )
"""

import io
import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from plugins.utils.aws_s3_utils import S3Handler

logger = logging.getLogger(__name__)


class S3GoldOperator(BaseOperator):
    """Aggregate silver brewery data into a gold-layer summary table.

    Reads every silver Parquet partition written for the current
    logical date, unions them, and produces a single aggregated file
    with the count of breweries per ``country``, ``state_province``,
    and ``brewery_type``.

    Attributes:
        ui_color (str): Gold hex colour shown in the Airflow Graph view.
        ui_fgcolor (str): Foreground colour for the task box.
        template_fields (tuple): Fields that support Jinja templating.

    Args:
        silver_layer_bucket (str): Bucket containing silver Parquet files.
            Defaults to ``"silver-layer"``.
        silver_layer_prefix (str): Key prefix for the silver dataset.
            Defaults to ``"openbreweries/breweries"``.
        gold_layer_bucket (str): Target bucket for gold output.
            Defaults to ``"gold-layer"``.
        gold_layer_prefix (str): Key prefix for the gold output file.
            Defaults to ``"openbreweries/breweries"``.
        aws_conn_id (str): Airflow Connection ID for MinIO/S3 credentials.
            Defaults to ``"minio_conn"``.
        **kwargs: Passed through to ``BaseOperator.__init__``.
    """

    template_fields = (
        "silver_layer_bucket",
        "silver_layer_prefix",
        "gold_layer_bucket",
        "gold_layer_prefix",
    )
    ui_color = "#FFD700"   # gold
    ui_fgcolor = "#000000"

    def __init__(
        self,
        silver_layer_bucket: str = "silver-layer",
        silver_layer_prefix: str = "openbreweries/breweries",
        gold_layer_bucket: str = "gold-layer",
        gold_layer_prefix: str = "openbreweries/breweries",
        aws_conn_id: str = "minio_conn",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.silver_layer_bucket = silver_layer_bucket
        self.silver_layer_prefix = silver_layer_prefix
        self.gold_layer_bucket = gold_layer_bucket
        self.gold_layer_prefix = gold_layer_prefix
        self.aws_conn_id = aws_conn_id

    def execute(self, context: dict) -> str:
        """Run the gold aggregation pipeline.

        Steps:

        1. List all silver Parquet keys for the current logical date.
        2. Read and union every partition into a single DataFrame.
        3. Aggregate: ``count(id)`` grouped by location and brewery type.
        4. Validate the aggregation result.
        5. Write a single Parquet file to the gold S3 bucket.

        Args:
            context (dict): Airflow task context.

        Returns:
            str: Full S3 URI of the written gold file. Automatically
                pushed to XCom.

        Raises:
            AirflowException: If no silver partitions are found for the
                logical date, or if any validation check fails.
        """
        s3_handler = S3Handler(conn_id=self.aws_conn_id)
        logical_date = str(context["logical_date"])[:10]  # "YYYY-MM-DD"

        # 1 — Discover all silver partition files for this logical date
        silver_keys = self._list_silver_keys(s3_handler, logical_date)
        logger.info(
            "Found %d silver partition(s) for %s", len(silver_keys), logical_date
        )

        # 2 — Read and union all partitions
        df = self._read_and_union_silver(s3_handler, silver_keys)
        logger.info("Unioned silver shape: %s", df.shape)

        # 3 — Aggregate
        df_gold = self._aggregate(df)
        logger.info("Gold shape: %s", df_gold.shape)

        # 4 — Validate
        self._validate(df_gold, total_silver_rows=len(df))

        # 5 — Write
        gold_uri = self._write_gold(s3_handler, df_gold, logical_date)
        logger.info("✓ Gold written to %s", gold_uri)
        return gold_uri

    # ------------------------------------------------------------------
    # Step 1 — Discover silver partition files
    # ------------------------------------------------------------------

    def _list_silver_keys(self, s3_handler: S3Handler, logical_date: str) -> List[str]:
        """List all silver Parquet object keys written for a given date.

        Scans every ``country=*/state_province=*/`` partition under the
        silver prefix and filters for files whose name matches the
        logical date (``YYYY-MM-DD.parquet``).

        Args:
            s3_handler (S3Handler): Configured S3 client wrapper.
            logical_date (str): Date string in ``YYYY-MM-DD`` format.

        Returns:
            List[str]: S3 object keys for all matching silver files.

        Raises:
            AirflowException: If no matching files are found, indicating
                the upstream silver task did not complete successfully.
        """
        paginator = s3_handler.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.silver_layer_bucket,
            Prefix=self.silver_layer_prefix,
        )

        keys = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(f"{logical_date}.parquet")
        ]

        if not keys:
            raise AirflowException(
                f"No silver files found for date {logical_date} under "
                f"s3://{self.silver_layer_bucket}/{self.silver_layer_prefix}. "
                "Ensure the silver task completed successfully."
            )

        return keys

    # ------------------------------------------------------------------
    # Step 2 — Read and union
    # ------------------------------------------------------------------

    def _read_and_union_silver(
        self, s3_handler: S3Handler, keys: List[str]
    ) -> pd.DataFrame:
        """Read all silver partition files and union them into one DataFrame.

        Each file is downloaded into a ``BytesIO`` buffer and read with
        ``pyarrow`` to guarantee consistent UTF-8 string encoding across
        all partitions.

        The ``country`` and ``state_province`` partition columns were
        dropped from the Parquet file content by the silver operator
        (they were encoded in the path). They are reconstructed here
        by parsing the S3 key path before reading the file, and
        re-attached as columns so the aggregation has access to them.

        Args:
            s3_handler (S3Handler): Configured S3 client wrapper.
            keys (List[str]): S3 object keys to read.

        Returns:
            pd.DataFrame: Unioned DataFrame with ``country`` and
                ``state_province`` columns restored.
        """
        frames: List[pd.DataFrame] = []

        for key in keys:
            country, state = self._extract_partition_values(key)

            s3_obj = s3_handler.s3.get_object(
                Bucket=self.silver_layer_bucket, Key=key
            )
            df_partition = pd.read_parquet(
                io.BytesIO(s3_obj["Body"].read()), engine="pyarrow"
            )

            # Restore partition columns stripped during silver write
            df_partition["country"] = country
            df_partition["state_province"] = state

            frames.append(df_partition)
            logger.info("Read partition %s (%d rows)", key, len(df_partition))

        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _extract_partition_values(key: str):
        """Parse ``country`` and ``state_province`` from an S3 partition key.

        Expects Hive-style path segments in the key, e.g.::

            openbreweries/breweries/country=united_states/state_province=california/2024-01-01.parquet

        Args:
            key (str): Full S3 object key.

        Returns:
            tuple[str, str]: ``(country, state_province)`` values.

        Raises:
            ValueError: If the key does not contain the expected
                Hive partition segments.
        """
        parts = key.split("/")
        try:
            country = next(p.split("=")[1] for p in parts if p.startswith("country="))
            state   = next(p.split("=")[1] for p in parts if p.startswith("state_province="))
        except StopIteration:
            raise ValueError(
                f"Could not extract partition values from key: {key}"
            )
        return country, state

    # ------------------------------------------------------------------
    # Step 3 — Aggregation
    # ------------------------------------------------------------------

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate brewery counts by location and type.

        Groups the unioned silver DataFrame by ``country``,
        ``state_province``, and ``brewery_type``, counting distinct
        brewery ``id`` values per group.

        Null ``brewery_type`` values (invalid types coerced to NaN by
        the silver operator) are excluded from the aggregation via
        ``dropna=True`` (default behaviour of ``groupby``).

        The result is sorted by ``country → state_province → quantity``
        descending, making it immediately readable in BI tools without
        additional sorting.

        Audit columns are attached for lineage tracking.

        Args:
            df (pd.DataFrame): Unioned silver DataFrame.

        Returns:
            pd.DataFrame: Aggregated DataFrame with columns:
                ``country``, ``state_province``, ``brewery_type``,
                ``quantity``, ``_gold_processed_at``.
        """
        logger.info("Aggregating by country, state_province, brewery_type")

        df_gold = (
            df.groupby(["country", "state_province", "brewery_type"], observed=True)
            .agg(quantity=("id", "count"))
            .reset_index()
            .sort_values(
                by=["country", "state_province", "quantity"],
                ascending=[True, True, False],
            )
            .reset_index(drop=True)
        )

        df_gold["_gold_processed_at"] = datetime.now(timezone.utc).isoformat()

        logger.info(
            "Aggregation complete. %d groups | total breweries: %d",
            len(df_gold),
            df_gold["quantity"].sum(),
        )
        return df_gold

    # ------------------------------------------------------------------
    # Step 4 — Validation
    # ------------------------------------------------------------------

    def _validate(self, df_gold: pd.DataFrame, total_silver_rows: int) -> None:
        """Assert correctness of the gold aggregation before writing.

        Rules:
            - Result must not be empty.
            - ``quantity`` must be positive for every row.
            - Sum of ``quantity`` must equal total silver row count —
              ensures no rows were lost or double-counted during
              aggregation.

        Args:
            df_gold (pd.DataFrame): Aggregated gold DataFrame.
            total_silver_rows (int): Total row count from the unioned
                silver DataFrame, used as the expected sum of quantities.

        Raises:
            AirflowException: If any validation rule fails.
        """
        errors: List[str] = []

        if df_gold.empty:
            errors.append("Gold aggregation produced an empty DataFrame.")

        if (df_gold["quantity"] <= 0).any():
            errors.append("One or more groups have a quantity of zero or less.")

        total_gold = int(df_gold["quantity"].sum())
        if total_gold != total_silver_rows:
            errors.append(
                f"Row count mismatch: silver had {total_silver_rows} rows "
                f"but gold quantities sum to {total_gold}. "
                "Possible duplicate or missing rows during aggregation."
            )

        if errors:
            for error in errors:
                logger.error("Gold validation failed: %s", error)
            raise AirflowException(
                f"Gold validation failed ({len(errors)} error(s)):\n"
                + "\n".join(errors)
            )

        logger.info("✓ Gold validation passed. %d aggregation groups.", len(df_gold))

    # ------------------------------------------------------------------
    # Step 5 — Write
    # ------------------------------------------------------------------

    def _write_gold(
        self,
        s3_handler: S3Handler,
        df_gold: pd.DataFrame,
        logical_date: str,
    ) -> str:
        """Serialise and upload the gold aggregation to S3.

        Writes a single Parquet file — no partitioning. The gold
        aggregation is compact enough that a single file is optimal
        for BI tools that read the full dataset for dashboard rendering.

        ``replace=True`` — gold is reprocessable. Re-running the DAG
        for a past date replaces the previous aggregation with the
        corrected one.

        Args:
            s3_handler (S3Handler): Configured S3 client wrapper.
            df_gold (pd.DataFrame): Validated gold aggregation DataFrame.
            logical_date (str): DAG logical date in ``YYYY-MM-DD`` format.

        Returns:
            str: Full S3 URI of the written gold file.
        """
        s3_key = f"{self.gold_layer_prefix}/{logical_date}.parquet"

        buffer = io.BytesIO()
        df_gold.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        s3_handler.s3.upload_fileobj(
            Fileobj=buffer,
            Bucket=self.gold_layer_bucket,
            Key=s3_key,
        )

        return f"s3://{self.gold_layer_bucket}/{s3_key}"