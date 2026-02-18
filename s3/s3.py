import boto3
import os
from botocore.config import Config

class ClientS3:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
        )
        self.bucket = bucket
    
    def upload_file(self, path, s3_key=None):
        if s3_key is None:
            s3_key = path
        try:
            self.s3_client.upload_file(path, self.bucket, s3_key)
            print(f'Файл {path} загружен в бакет {self.bucket}')
        except Exception as e:
            print(f'Произошла ошибка: {e}')
        
    def load_files(self, s3_path, local_path) -> None:
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=s3_path)
            if 'Contents' not in response:
                print(f"В папке {s3_path} пусто")
                return
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('_SUCCESS'):
                    continue
                file_name = os.path.basename(key)
                self.s3_client.download_file(self.bucket, key, os.path.join(local_path, file_name))
        except Exception as e:
            print(f'Произошла ошибка при скачивание результа: {e}')