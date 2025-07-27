import psycopg2
from sqlalchemy import create_engine, text

class DBConnection:
    
    def __init__(self, host, dbname, user, password, port):
        #Init Self
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port =  port
        self.engine = None

    def connect(self):
        try:
            db_connection_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            self.engine = create_engine(db_connection_str)
            print("Successfully connected to the PostgreSQL database.")
            return self.engine
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def close(self):
        self.engine.dispose()
        self.engine = None
        print("Database connection closed.")