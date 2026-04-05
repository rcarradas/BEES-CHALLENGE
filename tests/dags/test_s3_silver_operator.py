"""
test_s3_silver_operator.py
===========================
Unit tests for S3SilverOperator — each transform method tested in isolation.
"""

import io
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException

import plugins.operators.s3_silver_operator as silver_mod
from plugins.operators.s3_silver_operator import S3SilverOperator


def _make_operator(**overrides):
    defaults = dict(
        task_id="test_task",
        bronze_file_uri="s3://bronze-layer/openbreweries/breweries/ingestion_date=2024-01-01/20240101T030000Z.parquet",
        bronze_layer_bucket="bronze-layer",
        bronze_layer_prefix="openbreweries/breweries",
        silver_layer_bucket="silver-layer",
        silver_layer_prefix="openbreweries/breweries",
    )
    defaults.update(overrides)
    return S3SilverOperator(**defaults)


class TestCastTypes:

    def test_coordinates_coerced_to_float(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        assert df["longitude"].dtype == "float64"
        assert df["latitude"].dtype == "float64"

    def test_bad_coordinate_becomes_nan(self, sample_dataframe):
        op = _make_operator()
        sample_dataframe.loc[0, "longitude"] = "not_a_number"
        df = op._cast_types(sample_dataframe.copy())
        assert pd.isna(df.loc[0, "longitude"])

    def test_missing_schema_column_added_as_null(self, sample_dataframe):
        op = _make_operator()
        df_input = sample_dataframe.drop(columns=["address_2"])
        df = op._cast_types(df_input.copy())
        assert "address_2" in df.columns
        assert df["address_2"].isna().all()

    def test_id_cast_to_string_type(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        assert str(df["id"].dtype) == "string"


class TestNormalizeStringColumns:

    def test_strips_whitespace_from_string_columns(self, sample_dataframe):
        op = _make_operator()
        sample_dataframe.loc[0, "city"] = "  Portland  "
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        assert df.loc[0, "city"] == "Portland"

    def test_empty_string_becomes_none(self, sample_dataframe):
        op = _make_operator()
        sample_dataframe.loc[0, "phone"] = ""
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        assert pd.isna(df.loc[0, "phone"])

    def test_brewery_type_lowercased(self, sample_dataframe):
        op = _make_operator()
        sample_dataframe.loc[0, "brewery_type"] = "Micro"
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        assert df.loc[0, "brewery_type"] == "micro"

    def test_name_preserves_case(self, sample_dataframe):
        """Names must NOT be lowercased — display casing preserved."""
        op = _make_operator()
        sample_dataframe.loc[0, "name"] = "Sierra Nevada Brewing Co."
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        assert df.loc[0, "name"] == "Sierra Nevada Brewing Co."

    def test_invalid_brewery_type_becomes_nan(self, sample_dataframe):
        """Unknown types like 'taproom' coerced to NaN via pd.Categorical."""
        op = _make_operator()
        sample_dataframe.loc[0, "brewery_type"] = "taproom"
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        assert pd.isna(df.loc[0, "brewery_type"])


class TestHandleNulls:

    def test_row_with_null_id_is_dropped(self, sample_dataframe):
        op = _make_operator()
        sample_dataframe.loc[0, "id"] = None
        df = op._cast_types(sample_dataframe.copy())
        df = op._handle_nulls(df)
        assert len(df) == len(sample_dataframe) - 1

    def test_row_with_null_country_is_dropped(self, sample_dataframe):
        op = _make_operator()
        sample_dataframe.loc[0, "country"] = None
        df = op._cast_types(sample_dataframe.copy())
        df = op._handle_nulls(df)
        assert len(df) == len(sample_dataframe) - 1

    def test_null_optional_column_row_is_kept(self, sample_dataframe):
        """Null in phone (optional) must not drop the row."""
        op = _make_operator()
        sample_dataframe.loc[0, "phone"] = None
        df = op._cast_types(sample_dataframe.copy())
        df = op._handle_nulls(df)
        assert len(df) == len(sample_dataframe)

    def test_clean_data_unchanged(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._handle_nulls(df)
        assert len(df) == len(sample_dataframe)


class TestDeduplicate:

    def test_duplicate_id_kept_last(self, sample_dataframe):
        op = _make_operator()
        dupe = sample_dataframe.iloc[[0]].copy()
        dupe["name"] = "Updated Name"
        df_with_dupe = pd.concat([sample_dataframe, dupe], ignore_index=True)
        df = op._cast_types(df_with_dupe.copy())
        df = op._deduplicate(df)
        row = df[df["id"] == sample_dataframe.iloc[0]["id"]]
        assert len(row) == 1
        assert row.iloc[0]["name"] == "Updated Name"

    def test_no_duplicates_unchanged(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._deduplicate(df)
        assert len(df) == len(sample_dataframe)

    def test_index_reset_after_dedup(self, sample_dataframe):
        op = _make_operator()
        dupe = sample_dataframe.iloc[[0]].copy()
        df_with_dupe = pd.concat([sample_dataframe, dupe], ignore_index=True)
        df = op._cast_types(df_with_dupe.copy())
        df = op._deduplicate(df)
        assert list(df.index) == list(range(len(df)))


class TestDataQualityChecks:

    def test_passes_on_clean_data(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        df = op._handle_nulls(df)
        df = op._deduplicate(df)
        op._data_quality_checks(df)  # must not raise

    def test_raises_on_empty_dataframe(self):
        op = _make_operator()
        df = pd.DataFrame(columns=["id", "name", "brewery_type", "country"])
        with pytest.raises(AirflowException):
            op._data_quality_checks(df)

    def test_raises_on_duplicate_ids(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        # re-introduce duplicate after dedup to test the check directly
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        with pytest.raises(AirflowException, match="[Dd]uplicate"):
            op._data_quality_checks(df)


class TestWriteSilver:

    def test_creates_one_partition_per_country_state_combo(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        df = op._handle_nulls(df)
        df = op._deduplicate(df)
        mock_s3_client = MagicMock()
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        op._write_silver(mock_s3_handler, df, "2024-01-01")
        expected = df.groupby(["country", "state_province"], observed=True).ngroups
        assert mock_s3_client.upload_fileobj.call_count == expected

    def test_s3_key_contains_country_and_state(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        df = op._handle_nulls(df)
        df = op._deduplicate(df)
        mock_s3_client = MagicMock()
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        op._write_silver(mock_s3_handler, df, "2024-01-01")
        all_keys = [c.kwargs["Key"] for c in mock_s3_client.upload_fileobj.call_args_list]
        for key in all_keys:
            assert "country=" in key
            assert "state_province=" in key

    def test_returns_base_uri(self, sample_dataframe):
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        df = op._handle_nulls(df)
        df = op._deduplicate(df)
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = MagicMock()
        result = op._write_silver(mock_s3_handler, df, "2024-01-01")
        assert result == "s3://silver-layer/openbreweries/breweries"

    def test_partition_columns_not_in_parquet_content(self, sample_dataframe):
        """country and state_province must be dropped from file content."""
        op = _make_operator()
        df = op._cast_types(sample_dataframe.copy())
        df = op._normalize_string_columns(df)
        df = op._handle_nulls(df)
        df = op._deduplicate(df)
        uploaded = []

        def capture(Fileobj, Bucket, Key):
            Fileobj.seek(0)
            uploaded.append((Key, Fileobj.read()))

        mock_s3_client = MagicMock()
        mock_s3_client.upload_fileobj.side_effect = capture
        mock_s3_handler = MagicMock()
        mock_s3_handler.s3 = mock_s3_client
        op._write_silver(mock_s3_handler, df, "2024-01-01")

        for key, raw in uploaded:
            written_df = pd.read_parquet(io.BytesIO(raw), engine="pyarrow")
            assert "country" not in written_df.columns, f"country found in {key}"
            assert "state_province" not in written_df.columns, f"state_province found in {key}"