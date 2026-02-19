from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from s3.s3 import ClientS3

class StoreVitrina:
    def __init__(self, endpoint, access_key, secret_key) -> None:
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = self._create_session()
    
    def _create_session(self) -> SparkSession:
        conf = SparkConf()
        conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
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
        if data['orders'].isEmpty():
            print('Данные пустые')
            raise ValueError("Данные пустые в бакете")
        users, orders, stores = data['users'], data['orders'], data['stores']
        
        users = users.filter(F.year(users.created_at) == 2025)
        users = users.select(users.id, users.name.alias("user_name"))
        orders = orders.join(users, users.id == orders.user_id, 'inner')\
            .join(stores, stores.id == orders.store_id, 'inner')
        result = orders.groupBy(orders.city, orders.name.alias('store_name')).\
            agg(F.sum('amount').alias('target_amount'))
        window = Window.partitionBy("city").orderBy(result.target_amount.desc())
        
        return result.withColumn('dense_rank', F.dense_rank().over(window)) \
            .filter(F.col('dense_rank') <= 3).select(F.col('city'), F.col('store_name'), F.col('target_amount'))
        
    
    def load(self, result: DataFrame) -> None:
        result.write.mode('overwrite').parquet('s3a://final-data/result.parquet')

