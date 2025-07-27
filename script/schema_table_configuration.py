from sqlalchemy import text

class SchemaTableConfiguration:
    
    def __init__(self, db_engine):
        self.engine = db_engine

    def create_schema(self):    
        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    connection.execute(text("CREATE SCHEMA IF NOT EXISTS ecommerce"))
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
            with self.engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(create_table_sql))
            print("Table 'ecommerce.transaction' created.")
        except Exception as e:
            print(f"Error creating 'ecommerce.transaction' table: {e}")