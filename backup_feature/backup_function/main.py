import os
import json
import fastavro
from google.cloud import storage
from google.cloud.sql.connector import Connector
import datetime

# Cloud Storage details
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# Cloud SQL connection details
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")  # e.g., "project-id:region:instance-id"

# Google Cloud Storage client
storage_client = storage.Client()

# Initialize Cloud SQL Connector
connector = Connector()

def get_connection():
    """Establishes a secure connection to Cloud SQL using the Cloud SQL Python Connector."""
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",  # No need to import pymysql explicitly
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )

def backup_table_to_avro(table_name):
    """Backs up a MySQL table from Cloud SQL to Avro format and uploads it to GCS."""
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Fetch table data
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        if not rows:
            print(f"No data found in table {table_name}. Skipping backup.")
            return

        # Define Avro schema dynamically based on table structure
        schema = {
            "type": "record",
            "name": table_name,
            "fields": [{"name": col, "type": ["null", "string", "int", "float", "boolean"]} for col in rows[0].keys()]
        }

        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        avro_filename = f"{table_name}_backup_{timestamp}.avro"

        # Write Avro file
        with open(avro_filename, "wb") as avro_file:
            fastavro.writer(avro_file, schema, rows)

        print(f"Backup created: {avro_filename}")

        # Upload to GCS
        upload_to_gcs(avro_filename, f"backup/{avro_filename}")

        # Clean up local file
        os.remove(avro_filename)

    except Exception as e:
        print(f"Error backing up table {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

def upload_to_gcs(source_file, destination_blob):
    """Uploads a file to Google Cloud Storage."""
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)
        print(f"Uploaded {source_file} to gs://{BUCKET_NAME}/{destination_blob}")
    except Exception as e:
        print(f"Error uploading file to GCS: {e}")

def backup_function(cloud_event):
    """Backs up the tables"""
    tablenames = ['departments', 'jobs', 'hired_employees']
    for table in tablenames:
        backup_table_to_avro(table)