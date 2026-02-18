from pyspark.sql import SparkSession
from src.store_vitrina import StoreVitrina

def test_transform():
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    vitrina = StoreVitrina("http://localhost:9000", "user", "pass")
    
    u = spark.createDataFrame([(1, "User1", "2025-01-01")], ["id", "name", "created_at"])
    s = spark.createDataFrame([(10, "StoreA", "Moscow")], ["id", "name", "city"])
    o = spark.createDataFrame([(100, 500.0, 1, 10, "completed")], ["id", "amount", "user_id", "store_id", "status"])
    
    vitrina.read_data = lambda: {"users": u, "stores": s, "orders": o}
    res = vitrina.transform().collect()
    
    assert len(res) == 1
    assert res[0]['target_amount'] == 500.0
    assert res[0]['store_name'] == 'StoreA'
