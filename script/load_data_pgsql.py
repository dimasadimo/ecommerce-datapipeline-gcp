import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.types import TIMESTAMP, DATE, JSON
import os
from dotenv import load_dotenv 
import random

load_dotenv()

class PostgresDataLoad:

    def __init__(self, host, dbname, user, password, port):
        #Init Self
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port =  port
        self.conn = None
        self.cur = None
        self.engine = None

    def connect(self):
        try:
            db_connection_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            self.engine = create_engine(db_connection_str)
            self.conn = self.engine.connect()
            self.cur = self.conn.connection.cursor()
            print("Successfully connected to the PostgreSQL database.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def create_schema(self):    
        try:
            self.conn.execute(text("CREATE SCHEMA IF NOT EXISTS ecommerce"))
            self.conn.commit()
            print("Schema 'ecommerce' ensured to exist.")
        except Exception as e:
            print(f"Warning: Could not ensure schema ecommerce exists: {e}")
    
    def create_fact_table(self):
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ecommerce.transaction (
                transaction_id SERIAL PRIMARY KEY,
                created_at TIMESTAMP,
                customer_id INTEGER,
                session_id INTEGER,
                product_metadata JSONB,
                payment_method INTEGER,
                payment_status VARCHAR(50),
                shipment_fee INTEGER,
                shipment_location_lat FLOAT,
                shipment_location_long FLOAT,
                total_amount INTEGER
            );

            CREATE TABLE IF NOT EXISTS ecommerce.review (
                review_id SERIAL PRIMARY KEY,
                customer_id INTEGER,
                product_id INTEGER,
                rating INTEGER,
                review_text TEXT,
                sentiment_score FLOAT,
                is_flagged BOOLEAN,
                created_at TIMESTAMP
            );
            """
            self.conn.execute(text(create_table_sql))
            self.conn.commit()
            print("Table 'ecommerce.transaction' created.")
        except Exception as e:
            print(f"Error creating 'ecommerce.transaction' table: {e}")

    def load_data(self, csv_data_master):
        for csv_file, table_name in csv_data_master.items():
            print(f"\nProcessing file: '{csv_file}' into table: '{table_name}'")

            try:
                df = pd.read_csv(csv_file)

                dtype_mapping = {}
                dtype_mapping['created_at'] = TIMESTAMP

                if table_name == 'customer':
                    dtype_mapping['first_join_date'] = DATE
                    dtype_mapping['birthdate'] = DATE
                
                if table_name == 'sessions':
                    dtype_mapping['start_time'] = TIMESTAMP
                    dtype_mapping['end_time'] = TIMESTAMP

                df.to_sql(table_name, self.conn, schema="ecommerce", if_exists='replace', index=False, dtype=dtype_mapping)
                self.conn.commit()
                print(f"Data successfully loaded from '{csv_file}' into table '{table_name}'.")
            except Exception as e:
                print(f"An unexpected error occurred while processing {csv_file}: {e}")

    def add_primary_keys(self, primary_key_config):
        for table_name, pk_column in primary_key_config.items():
            try:
                alter_table_sql = f"ALTER TABLE ecommerce.{table_name} ADD PRIMARY KEY ({pk_column});"
                self.conn.execute(text(alter_table_sql))
                self.conn.commit()
            except Exception as e:
                print(f"Warning: Could not add primary key to table '{table_name}': {e}")
        
        print("Primary key added to table .")


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
            df_transactions.to_sql('transaction', self.conn, schema="ecommerce", if_exists='append', index=False, dtype=dtype_mapping_transactions)
            self.conn.commit()
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
            df_reviews.to_sql('review', self.conn, schema="ecommerce", if_exists='append', index=False, dtype=dtype_mapping_reviews)
            self.conn.commit()
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
                self.conn.execute(text(alter_table_sql))
                self.conn.commit()
            except Exception as e:
                print(f"Warning: Could not add foreign key: {e}")
        

        print("Foreign keys added to the table.")

    def close(self):
        self.cur.close()
        print("Cursor closed.")
        self.conn.close()
        print("Database connection closed.")

if __name__ == "__main__":

    csv_file = {
        f"{os.getenv('FILE_CSV_PATH')}customer.csv": "customer",
        f"{os.getenv('FILE_CSV_PATH')}product.csv": "product",
        f"{os.getenv('FILE_CSV_PATH')}payment_method.csv": "payment_method",
        f"{os.getenv('FILE_CSV_PATH')}sessions.csv": "sessions",
    }

    data_load = PostgresDataLoad(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port= os.getenv("DB_PORT")
    )

    primary_keys_config = {
        "customer": "customer_id",
        "product": "id",
        "payment_method": "payment_method_id",
        "sessions": "session_id",
    }

    foreign_keys_config = [
        {
            'table': 'transaction',
            'column': 'customer_id',
            'referenced_table': 'customer',
            'referenced_column': 'customer_id'
        },
        {
            'table': 'transaction',
            'column': 'session_id',
            'referenced_table': 'sessions',
            'referenced_column': 'session_id'
        },
        {
            'table': 'transaction',
            'column': 'payment_method',
            'referenced_table': 'payment_method',
            'referenced_column': 'payment_method_id'
        },
        {
            'table': 'review',
            'column': 'customer_id',
            'referenced_table': 'customer',
            'referenced_column': 'customer_id'
        },
        {
            'table': 'review',
            'column': 'product_id',
            'referenced_table': 'product',
            'referenced_column': 'id'
        },
    ]

    try:
        data_load.connect()
        data_load.create_schema()
        data_load.create_fact_table()
        data_load.load_data(csv_file)
        data_load.add_primary_keys(primary_keys_config)

        data_load.load_transactions()
        data_load.load_reviews()
        data_load.add_foreign_keys(foreign_keys_config)

    except Exception as e:
        print(f"An error occurred during the data loading process: {e}")
    finally:
        data_load.close()
