"""
test_s3_bronze_operator.py
===========================
Unit tests for S3BronzeOperator.
S3Handler and OpenBreweryHook are mocked — no real S3 or HTTP calls.
"""

import re
import pytest
from unittest.mock import MagicMock, patch

import plugins.operators.s3_bronze_operator as bronze_mod
from plugins.operators.s3_bronze_operator import S3BronzeOperator


def _make_operator(**overrides):
    """Instantiate S3BronzeOperator with safe test defaults."""
    defaults = dict(
        task_id="test_task",
        s3_bucket="bronze-layer",
        s3_prefix="openbreweries/breweries",
        aws_conn_id="minio_conn",
        openbrewery_url="https://api.openbrewerydb.org/v1/breweries",
        max_retries=3,
        register_per_page=200,
        exponential_backoff_seconds=2,
    )
    defaults.update(overrides)
    return S3BronzeOperator(**defaults)


class TestConvertToDataframe:

    def test_audit_columns_added(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        df = op._convert_to_dataframe(sample_brewery_records, mock_airflow_context)
        for col in ["_ingested_at", "_source", "_dag_id", "_dag_run_id", "_logical_date"]:
            assert col in df.columns

    def test_source_column_matches_url(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        df = op._convert_to_dataframe(sample_brewery_records, mock_airflow_context)
        assert (df["_source"] == op.openbrewery_url).all()

    def test_dag_id_matches_context(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        df = op._convert_to_dataframe(sample_brewery_records, mock_airflow_context)
        assert (df["_dag_id"] == "test_dag").all()

    def test_row_count_matches_input(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        df = op._convert_to_dataframe(sample_brewery_records, mock_airflow_context)
        assert len(df) == len(sample_brewery_records)

    def test_source_columns_preserved(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        df = op._convert_to_dataframe(sample_brewery_records, mock_airflow_context)
        for col in ["id", "name", "brewery_type", "country"]:
            assert col in df.columns


class TestBuildS3Key:

    def test_key_starts_with_prefix(self):
        op = _make_operator()
        assert op._build_s3_key().startswith("openbreweries/breweries/")

    def test_key_contains_hive_partition(self):
        op = _make_operator()
        assert "ingestion_date=" in op._build_s3_key()

    def test_key_ends_with_parquet(self):
        op = _make_operator()
        assert op._build_s3_key().endswith(".parquet")

    def test_key_pattern_matches_expected_format(self):
        op = _make_operator()
        pattern = r"openbreweries/breweries/ingestion_date=\d{4}-\d{2}-\d{2}/\d{8}T\d{6}Z\.parquet"
        assert re.match(pattern, op._build_s3_key()), f"Key '{op._build_s3_key()}' does not match pattern"


class TestExecute:

    def test_execute_returns_s3_uri(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        mock_s3_handler = MagicMock()
        mock_hook = MagicMock()
        mock_hook.get_breweries.return_value = sample_brewery_records
        with patch.object(bronze_mod, "S3Handler", return_value=mock_s3_handler), \
             patch.object(bronze_mod, "OpenBreweryHook", return_value=mock_hook):
            result = op.execute(mock_airflow_context)
        assert result.startswith("s3://bronze-layer/")
        assert result.endswith(".parquet")

    def test_execute_calls_upload_fileobj(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        mock_s3_client = MagicMock()
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        mock_hook = MagicMock()
        mock_hook.get_breweries.return_value = sample_brewery_records
        with patch.object(bronze_mod, "S3Handler", return_value=mock_s3_handler), \
             patch.object(bronze_mod, "OpenBreweryHook", return_value=mock_hook):
            op.execute(mock_airflow_context)
        mock_s3_client.upload_fileobj.assert_called_once()

    def test_execute_raises_on_empty_api_response(self, mock_airflow_context):
        op = _make_operator()
        mock_hook = MagicMock()
        mock_hook.get_breweries.return_value = []
        with patch.object(bronze_mod, "S3Handler"), \
             patch.object(bronze_mod, "OpenBreweryHook", return_value=mock_hook):
            with pytest.raises(ValueError, match="API returned zero records — aborting bronze ingestion."):
                op.execute(mock_airflow_context)

    def test_upload_bucket_matches_config(self, sample_brewery_records, mock_airflow_context):
        op = _make_operator()
        mock_s3_client = MagicMock()
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        mock_hook = MagicMock()
        mock_hook.get_breweries.return_value = sample_brewery_records
        with patch.object(bronze_mod, "S3Handler", return_value=mock_s3_handler), \
             patch.object(bronze_mod, "OpenBreweryHook", return_value=mock_hook):
            op.execute(mock_airflow_context)
        _, kwargs = mock_s3_client.upload_fileobj.call_args
        assert kwargs["Bucket"] == "bronze-layer"