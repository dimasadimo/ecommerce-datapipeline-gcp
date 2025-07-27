from sqlalchemy import text
from sqlalchemy.types import TIMESTAMP, DATE, JSON
import random
import pandas as pd

class LoadFactData:
    
    def __init__(self, db_engine):
        self.engine = db_engine

    def load_transactions(self):
        print("\nGenerating and loading 500 transactions...")

        transactions_data = []

        for i in range(500):

            quantity = random.randint(1, 10)
            item_price = random.randint(50000, 200000)
            total_price = quantity * item_price
            shipment_fee = random.randint(5000, 50000)


            transactions_data.append({
                'created_at': pd.Timestamp.today(),
                'customer_id': random.randint(1, 100000),
                'session_id': random.randint(1, 50000),
                'product_metadata': [{
                    'product_id': random.randint(1, 60000),
                    'quantity': quantity,
                    'item_price': item_price
                }], 
                'payment_method': random.randint(1, 15),
                'payment_status': 'Success' if random.random() < 0.9 else 'Failed',
                'shipment_fee': shipment_fee,
                'shipment_location_lat': round(random.uniform(-90.0, 90.0), 6),
                'shipment_location_long': round(random.uniform(-180.0, 180.0), 6),
                'total_amount': total_price + shipment_fee
            })
        
        df_transactions = pd.DataFrame(transactions_data)

        dtype_mapping_transactions = {
            'created_at': TIMESTAMP,
            'product_metadata': JSON,
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    df_transactions.to_sql('transaction', connection, schema="ecommerce", if_exists='append', index=False, dtype=dtype_mapping_transactions)
            print("Successfully loaded 500 transactions into 'ecommerce.transaction'.")
        except Exception as e:
            print(f"An error occurred while loading transactions: {e}")
    
    def load_reviews(self):
        print("\nGenerating and loading 500 reviews...")

        review_data = []

        phrases = [
            "Amazing product", "Really loved it", "Exceeded my expectations", "Great quality", 
            "Will definitely buy again", "Fast delivery", "Highly recommend", "Super useful", "Arrived damaged", 
            "Too expensive for the quality", "Customer service was unhelpful", "It's okay", "As expected", "Product is decent", "Not bad", 
            "Average experience", "Met my needs", "No issues so far", None
        ]
        
        for i in range(500):

            review_data.append({
                'customer_id': random.randint(1, 100000),
                'product_id': random.randint(1, 60000),
                'rating': random.randint(1, 5),
                'review_text': random.choice(phrases),
                'sentiment_score': round(random.uniform(-1, 1), 1),
                'is_flagged': True if random.random() < 0.9 else False,
                'created_at': pd.Timestamp.today(),
            })
        
        df_reviews = pd.DataFrame(review_data)

        dtype_mapping_reviews = {
            'created_at': TIMESTAMP,
        }

        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    df_reviews.to_sql('review', connection, schema="ecommerce", if_exists='append', index=False, dtype=dtype_mapping_reviews)
            print("Successfully loaded 500 reviews into 'ecommerce.review'.")
        except Exception as e:
            print(f"An error occurred while loading reviews: {e}")


    def add_foreign_keys(self, foreign_keys_config):
        for fk_details in foreign_keys_config:
            try:
                alter_table_sql = f"""
                ALTER TABLE ecommerce.{fk_details['table']}
                ADD CONSTRAINT fk_{fk_details['table']}_{fk_details['column']} FOREIGN KEY ({fk_details['column']})
                REFERENCES ecommerce.{fk_details['referenced_table']} ({fk_details['referenced_column']});
                """
                with self.engine.connect() as connection:
                    with connection.begin():
                        connection.execute(text(alter_table_sql))
            except Exception as e:
                print(f"Warning: Could not add foreign key: {e}")
        

        print("Foreign keys added to the table.")

