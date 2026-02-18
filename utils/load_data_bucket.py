import os
from s3.s3 import ClientS3
from utils.generate_data import main as generate

def upload_files(files: list, bucket: str) -> None:
    skip_file = [i for i in files if not os.path.exists('data/' + i)]
    if skip_file:
        print('Генерация данных')
        generate()
        print('Генерация завершена')
        
    client = ClientS3("http://localhost:9000",
                         "user",
                         "password123",
                         bucket)
    
    for file in files:
        client.upload_file('data/' + file)

if __name__ == '__main__':
    upload_files(['users.parquet', 'orders.parquet', 'stores.parquet'], 'raw-data')