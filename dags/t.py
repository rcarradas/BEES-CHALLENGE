from minio import Minio
from minio.error import S3Error
import time


if __name__ == '__main__':
    minioClient = Minio("localhost:9000",
                        access_key="admin",
                        secret_key="password",
                        secure=False)
    try:
        minioClient.make_bucket("testebucket")
        print("Bucket criado com sucesso")
    except S3Error as err:
        print(err)
    except Exception as err:
        print(err)