import os
from src.store_vitrina import StoreVitrina
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
        
def main():
    try:
        upload_files(['users.parquet', 'orders.parquet', 'stores.parquet'], 'raw-data')
        vitrina = StoreVitrina("http://localhost:9000",
                               "user",
                               "password123")
        vitrina.load(vitrina.transform())
        print("Данные выгружены")
        
        client = ClientS3("http://localhost:9000",
                         "user",
                         "password123",
                         'final-data')
        client.load_files('result.parquet', 'data')
        print(f'Данные загружены физически на компьютер')
    except Exception as e:
        print(f'Произошла ошибка: {e}')


if __name__ == '__main__':
    main()