from src.store_vitrina import StoreVitrina
from s3.s3 import ClientS3

def main():
    try:
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