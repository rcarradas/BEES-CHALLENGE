import boto3
from airflow.hooks.base import BaseHook
import logging

logger = logging.getLogger(__name__)

class S3Handler:
    def __init__(self, conn_id : str = "minio_conn") -> boto3.client:
        self.s3 = self._get_minio_base_hook(conn_id=conn_id)
    
    @staticmethod
    def _get_minio_base_hook(conn_id: str) -> boto3.client:
        minio = BaseHook.get_connection(conn_id=conn_id).extra_dejson

        s3 = boto3.client(
                "s3",
                aws_access_key_id=minio['aws_access_key_id'],
                aws_secret_access_key=minio['aws_secret_access_key'],
                endpoint_url=minio['endpoint_url'],
        )
        return s3
