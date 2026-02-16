from faker import Faker
from datetime import datetime
import numpy as np
import pandas as pd
import random

class DataGenerator():
    def __init__(self):
        self.fake = Faker()
    
    def generate_users(self, n: int) -> pd.DataFrame:
        names = [self.fake.name() for _ in range(n)]
        id_list = np.arange(1, n + 1)
        phones = [self.fake.phone_number() for _ in range(n)]
        timestamps = [self.fake.date_time_between(start_date=datetime(2024, 1, 1), \
            end_date=datetime(2025, 12, 31)) for _ in range(n)]
        return pd.DataFrame({'id': id_list, 'name': names, 'phone': phones, 'created_at': timestamps})       

    def generate_store(self, n: int) -> pd.DataFrame:
        company = [self.fake.company() for _ in range(n)]
        id_list = np.arange(1, n + 1)
        available_cities = ['Moscow', 'Sochi', 'Kazan', 'St.Petersburg', 'Novosibirsk', 'Saransk', 'Ekaterinburg']
        city = [np.random.choice(available_cities) for _ in range(n)]
        return pd.DataFrame({"id": id_list, "name": company, "city": city})

    def generate_order(self, n: int, index_store: list, index_users: list)\
        -> pd.DataFrame:
        id_list = np.arange(1, n + 1)
        amount = [round(random.uniform(100, 10000), 2) for _ in range(n)]
        timestamps = [self.fake.date_time_between(start_date=datetime(2025,1,1), \
            end_date=datetime(2025, 12, 31)) for _ in range(n)]
        status = [np.random.choice(['completed', 'no completed']) for _ in range(n)]
        users = [np.random.choice(index_users) for _ in range(n)]
        stores = [np.random.choice(index_store) for _ in range(n)]
        return pd.DataFrame({'id': id_list, 'amount': amount, 'user_id': users, 'store_id': stores, \
            'status': status, 'created_at': timestamps})
        
    def generate_all(self, n: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        users = self.generate_users(n)
        stores = self.generate_store(n)
        orders = self.generate_order(n, stores['id'], users['id'])
        return users, stores, orders
    
def main():
    generator = DataGenerator()
    
    users_df, stores_df, orders_df = generator.generate_all(200)
    
    users_df.to_parquet('data/users.parquet', index=False)
    stores_df.to_parquet('data/stores.parquet', index=False)
    orders_df.to_parquet('data/orders.parquet', index=False)
    
    print("Паркеты созданы в папку data")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Произошла ошибка при создании: {e}")