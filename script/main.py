import os
from dotenv import load_dotenv

from db_connection import DBConnection
from schema_table_configuration import SchemaTableConfiguration
from load_dim_data import LoadDimData
from load_fact_data import LoadFactData
import sys

load_dotenv()

if __name__ == "__main__":
    
    csv_file = {
        f"{os.getenv('FILE_CSV_PATH')}customer.csv": "customer",
        f"{os.getenv('FILE_CSV_PATH')}product.csv": "product",
        f"{os.getenv('FILE_CSV_PATH')}payment_method.csv": "payment_method",
        f"{os.getenv('FILE_CSV_PATH')}sessions.csv": "sessions",
    }

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
        db_connector = DBConnection(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        db_engine = db_connector.connect()

        print(f"Type of db_engine: {type(db_engine)}")

        schema_table_config = SchemaTableConfiguration(db_engine)
        schema_table_config.create_schema()
        schema_table_config.create_fact_table()

        load_dim_data = LoadDimData(db_engine)
        load_dim_data.load_data(csv_file)
        load_dim_data.add_primary_keys(primary_keys_config)
        

        load_fact_data = LoadFactData(db_engine)
        load_fact_data.load_transactions()
        load_fact_data.load_reviews()
        # load_fact_data.add_foreign_keys(foreign_keys_config)

    except Exception as e:
        print(f"An error occurred during the overall data loading process: {e}")
        sys.exit(1)
    finally:
        db_connector.close()

