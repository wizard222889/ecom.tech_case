from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class StoreVitrina:
    def __init__(self, endpoint, access_key, secret_key) -> None:
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = self._create_session()
    
    def _create_session(self) -> SparkSession:
        conf = SparkConf()
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        conf.set("spark.hadoop.fs.s3a.access.key", self.access_key)
        conf.set("spark.hadoop.fs.s3a.secret.key", self.secret_key)
        conf.set("spark.hadoop.fs.s3a.endpoint", self.endpoint)
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        
        session = SparkSession.builder.config(conf=conf).getOrCreate()
        return session
    
    def read_data(self) -> dict:
        return {"users": self.session.read.parquet("s3a://raw-data/data/users.parquet"),
                "orders": self.session.read.parquet("s3a://raw-data/data/orders.parquet"),
                "stores": self.session.read.parquet("s3a://raw-data/data/stores.parquet")}
    
    
    def transform(self) -> DataFrame:
        data = self.read_data()
        users, orders, stores = data['users'], data['orders'], data['stores']
        users = users.select(users.name.alias("user_name"))
        users = users.filter(F.year(users.created_at) == 2025)
        orders = orders.join(users, users.user_name == orders.user_id, 'inner')\
            .join(stores, stores.id == orders.store_id, 'inner')
        result = orders.groupBy(orders.city, stores.name.alias('store_name')).\
            agg(F.sum('amount').alias('target_amount'))
        window = Window.partitionBy("city").orderBy(result.target_amount.desc())
        
        return result.withColumn('dense_rank', F.dense_rank().over(window)) \
            .filter(result.dense_rank <= 3)
        
    
    def load(self, result: DataFrame) -> None:
        result.write.mode('overwrite').parquet('s3a://final-data/result.parquet')

if __name__ == '__main__':
    try:
        vitrina = StoreVitrina("http://localhost:9000",
                               "user",
                               "password123")
        vitrina.load(vitrina.transform())
        print("Данные выгружены")
    except Exception as e:
        print(f'Произошла ошибка: {e}')