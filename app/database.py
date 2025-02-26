import os
import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

# Get environment variables (set these in Cloud Run)
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")  # e.g., project:region:instance
DB_USER = os.getenv("DB_USER")  # e.g., myuser
DB_PASS = os.getenv("DB_PASS")  # Use Secret Manager instead of hardcoding
DB_NAME = os.getenv("DB_NAME")  # e.g., employees_db

connector = Connector()

def getconn():
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        driver="pymysql",
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        ip_type=IPTypes.PRIVATE  # Use private IP for better security
    )

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine("mysql+pymysql://", creator=getconn)