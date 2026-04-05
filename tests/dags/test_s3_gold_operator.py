"""
test_s3_gold_operator.py
=========================
Unit tests for S3GoldOperator.
"""

import io
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

import plugins.operators.s3_gold_operator as gold_mod
from plugins.operators.s3_gold_operator import S3GoldOperator


def _make_operator(**overrides):
    defaults = dict(
        task_id="test_task",
        silver_layer_bucket="silver-layer",
        silver_layer_prefix="openbreweries/breweries",
        gold_layer_bucket="gold-layer",
        gold_layer_prefix="openbreweries/breweries",
        aws_conn_id="minio_conn",
    )
    defaults.update(overrides)
    return S3GoldOperator(**defaults)


def _make_silver_df():
    return pd.DataFrame([
        {"id": "b1", "name": "A", "brewery_type": "micro",   "country": "united_states", "state_province": "california"},
        {"id": "b2", "name": "B", "brewery_type": "micro",   "country": "united_states", "state_province": "california"},
        {"id": "b3", "name": "C", "brewery_type": "brewpub", "country": "united_states", "state_province": "california"},
        {"id": "b4", "name": "D", "brewery_type": "micro",   "country": "united_states", "state_province": "texas"},
        {"id": "b5", "name": "E", "brewery_type": "nano",    "country": "germany",       "state_province": "bavaria"},
    ])


class TestExtractPartitionValues:

    def test_parses_country_and_state(self):
        key = "openbreweries/breweries/country=united_states/state_province=california/2024-01-01.parquet"
        country, state = S3GoldOperator._extract_partition_values(key)
        assert country == "united_states"
        assert state == "california"

    def test_parses_multi_word_state(self):
        key = "openbreweries/breweries/country=united_states/state_province=new_york/2024-01-01.parquet"
        _, state = S3GoldOperator._extract_partition_values(key)
        assert state == "new_york"

    def test_raises_on_malformed_key(self):
        with pytest.raises(ValueError, match="Could not extract"):
            S3GoldOperator._extract_partition_values("openbreweries/breweries/2024-01-01.parquet")


class TestAggregate:

    def test_correct_quantity_per_group(self):
        op = _make_operator()
        df = _make_silver_df()
        result = op._aggregate(df)
        ca_micro = result[
            (result["country"] == "united_states") &
            (result["state_province"] == "california") &
            (result["brewery_type"] == "micro")
        ]["quantity"].iloc[0]
        assert ca_micro == 2

    def test_total_quantity_equals_input_rows(self):
        op = _make_operator()
        df = _make_silver_df()
        result = op._aggregate(df)
        assert result["quantity"].sum() == len(df)

    def test_output_contains_required_columns(self):
        op = _make_operator()
        result = op._aggregate(_make_silver_df())
        for col in ["country", "state_province", "brewery_type", "quantity", "_gold_processed_at"]:
            assert col in result.columns

    def test_null_brewery_type_excluded(self):
        op = _make_operator()
        df = _make_silver_df()
        df.loc[0, "brewery_type"] = None
        result = op._aggregate(df)
        assert None not in result["brewery_type"].values

    def test_sorted_by_quantity_desc_within_location(self):
        op = _make_operator()
        result = op._aggregate(_make_silver_df())
        ca = result[
            (result["country"] == "united_states") &
            (result["state_province"] == "california")
        ]
        quantities = ca["quantity"].tolist()
        assert quantities == sorted(quantities, reverse=True)


class TestValidate:

    def test_passes_on_valid_aggregation(self):
        op = _make_operator()
        df = _make_silver_df()
        gold = op._aggregate(df)
        op._validate(gold, total_silver_rows=len(df))  # must not raise

    def test_raises_on_empty_gold(self):
        op = _make_operator()
        empty = pd.DataFrame(columns=["country", "state_province", "brewery_type", "quantity"])
        with pytest.raises(AirflowException):
            op._validate(empty, total_silver_rows=5)

    def test_raises_on_row_count_mismatch(self):
        op = _make_operator()
        gold = op._aggregate(_make_silver_df())
        with pytest.raises(AirflowException, match="[Mm]ismatch"):
            op._validate(gold, total_silver_rows=999)

    def test_raises_when_quantity_zero(self):
        op = _make_operator()
        df = pd.DataFrame([{
            "country": "test", "state_province": "test",
            "brewery_type": "micro", "quantity": 0,
            "_gold_processed_at": "2024-01-01"
        }])
        with pytest.raises(AirflowException):
            op._validate(df, total_silver_rows=0)


class TestWriteGold:

    def test_write_uploads_single_file(self):
        op = _make_operator()
        gold = op._aggregate(_make_silver_df())
        mock_s3_client = MagicMock()
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        op._write_gold(mock_s3_handler, gold, "2024-01-01")
        assert mock_s3_client.upload_fileobj.call_count == 1

    def test_write_returns_correct_uri(self):
        op = _make_operator()
        gold = op._aggregate(_make_silver_df())
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = MagicMock()
        result = op._write_gold(mock_s3_handler, gold, "2024-01-01")
        assert result == "s3://gold-layer/openbreweries/breweries/2024-01-01.parquet"

    def test_write_key_contains_logical_date(self):
        op = _make_operator()
        gold = op._aggregate(_make_silver_df())
        mock_s3_client = MagicMock()
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        op._write_gold(mock_s3_handler, gold, "2024-01-01")
        _, kwargs = mock_s3_client.upload_fileobj.call_args
        assert "2024-01-01.parquet" in kwargs["Key"]

    def test_parquet_readable_after_write(self):
        """Buffer written to S3 must deserialize cleanly as valid Parquet."""
        op = _make_operator()
        gold = op._aggregate(_make_silver_df())
        captured = {}

        def capture(Fileobj, Bucket, Key):
            Fileobj.seek(0)
            captured["data"] = Fileobj.read()

        mock_s3_client = MagicMock()
        mock_s3_client.upload_fileobj.side_effect = capture
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        op._write_gold(mock_s3_handler, gold, "2024-01-01")
        result_df = pd.read_parquet(io.BytesIO(captured["data"]), engine="pyarrow")
        assert len(result_df) == len(gold)
        assert "quantity" in result_df.columns