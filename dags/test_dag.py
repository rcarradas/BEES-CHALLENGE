from airflow.decorators import dag, task
from datetime import datetime
from airflow.hooks.base import BaseHook
import boto3


@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def test_dag():
    @task
    def test_task():
        return "test"


    @task
    def test_task2():
        return "test2"


    @task
    def test_task3():
        return "test3"

    task1 = test_task()
    task2 = test_task2()
    task3 = test_task3()

    @task
    def create_minio_bucket():
        minio = BaseHook.get_connection('minio_conn').extra_dejson

        print(minio['endpoint_url'])
        print(minio['aws_access_key_id'])
        print(minio['aws_secret_access_key'])

        s3 = boto3.client(
                "s3",
                aws_access_key_id=minio['aws_access_key_id'],
                aws_secret_access_key=minio['aws_secret_access_key'],
                endpoint_url=minio['endpoint_url'],
        )

        try:
            s3.create_bucket(Bucket='testebucket-airflow')
            print("Bucket criado com sucesso")
        except Exception as err:
            print(err)



    task1 >> task2 >> task3
    create_minio_bucket()

test_dag()