"""
aws_s3_utils.py
================
Utility class for interacting with S3-compatible object storage.

This module provides ``S3Handler``, a thin wrapper around a ``boto3``
client configured from an Airflow Connection. It is designed to support
MinIO in local development while remaining compatible with AWS S3 in
production by simply changing the connection's ``endpoint_url``.

Example:
    Typical usage inside an Airflow Operator's ``execute`` method::

        from plugins.utils.aws_s3_utils import S3Handler

        s3_handler = S3Handler(conn_id="minio_conn")
        s3_handler.s3.upload_fileobj(Fileobj=buffer, Bucket="my-bucket", Key="path/file.parquet")

Note:
    ``S3Handler`` must be instantiated inside ``execute()``, never in
    ``__init__()``. The underlying ``boto3.client`` is not serializable,
    and Airflow serializes Operator objects when scheduling tasks.
"""

import logging

import boto3
from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)


class S3Handler:
    """Thin wrapper that exposes a configured ``boto3`` S3 client.

    Reads AWS credentials and endpoint configuration from an Airflow
    Connection's ``extra`` JSON field, keeping secrets out of source
    code and enabling per-environment credential management through
    the Airflow UI.

    The expected ``extra`` JSON structure on the Airflow Connection is::

        {
            "aws_access_key_id": "...",
            "aws_secret_access_key": "...",
            "endpoint_url": "http://minio:9000"
        }

    Attributes:
        s3 (boto3.client): Configured S3 client ready for API calls.

    Args:
        conn_id (str): Airflow Connection ID to read credentials from.
            Defaults to ``"minio_conn"``.
    """

    def __init__(self, conn_id: str = "minio_conn") -> None:
        self.s3 = self._get_minio_base_hook(conn_id=conn_id)

    @staticmethod
    def _get_minio_base_hook(conn_id: str) -> boto3.client:
        """Build and return a ``boto3`` S3 client from an Airflow Connection.

        Args:
            conn_id (str): The Airflow Connection ID whose ``extra`` JSON
                contains ``aws_access_key_id``, ``aws_secret_access_key``,
                and ``endpoint_url``.

        Returns:
            boto3.client: A configured S3 client instance.

        Raises:
            KeyError: If any of the required keys are missing from the
                connection's ``extra`` JSON.
            airflow.exceptions.AirflowNotFoundException: If ``conn_id``
                does not exist in the Airflow metadata database.
        """
        minio = BaseHook.get_connection(conn_id=conn_id).extra_dejson

        logger.info("Building S3 client for connection '%s' at '%s'", conn_id, minio.get("endpoint_url"))

        return boto3.client(
            "s3",
            aws_access_key_id=minio["aws_access_key_id"],
            aws_secret_access_key=minio["aws_secret_access_key"],
            endpoint_url=minio["endpoint_url"],
        )