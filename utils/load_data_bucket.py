from s3.s3 import ClientS3

def upload_files(files: list, bucket: str) -> None:
    client = ClientS3("http://localhost:9000",
                         "user",
                         "password123",
                         bucket)
    for file in files:
        client.upload_file('data/' + file)

if __name__ == '__main__':
    upload_files(['users.parquet', 'orders.parquet', 'stores.parquet'], 'raw-data')