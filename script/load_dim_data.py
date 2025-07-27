from sqlalchemy import text
from sqlalchemy.types import TIMESTAMP, DATE
import pandas as pd

class LoadDimData:
    
    def __init__(self, db_engine):
        self.engine = db_engine

    def load_data(self, csv_data_master):
        for csv_file, table_name in csv_data_master.items():
            print(f"\nProcessing file: '{csv_file}' into table: '{table_name}'")

            try:
                df = pd.read_csv(csv_file)

                df['created_at'] = pd.to_datetime(df['created_at'])

                dtype_mapping = {}
                dtype_mapping['created_at'] = TIMESTAMP

                if table_name == 'customer':
                    dtype_mapping['first_join_date'] = DATE
                    dtype_mapping['birthdate'] = DATE
                
                if table_name == 'sessions':
                    dtype_mapping['start_time'] = TIMESTAMP
                    dtype_mapping['end_time'] = TIMESTAMP

                with self.engine.connect() as connection:
                    with connection.begin():
                        df.to_sql(table_name, connection, schema="ecommerce", if_exists='replace', index=False, dtype=dtype_mapping)
                        
                print(f"Data successfully loaded from '{csv_file}' into table '{table_name}'.")
            except Exception as e:
                print(f"An unexpected error occurred while processing {csv_file}: {e}")

    def add_primary_keys(self, primary_key_config):
        for table_name, pk_column in primary_key_config.items():
            try:
                alter_table_sql = f"ALTER TABLE ecommerce.{table_name} ADD PRIMARY KEY ({pk_column});"
                with self.engine.connect() as connection:
                    with connection.begin():
                        connection.execute(text(alter_table_sql))
            except Exception as e:
                print(f"Warning: Could not add primary key to table '{table_name}': {e}")
        
        print("Primary key added to table .")

