import functions_framework
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
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")  # e.g., "project-id:region:instance-id"

# Google Cloud Storage client
storage_client = storage.Client()


def get_connection():
    # Initialize Cloud SQL Connector
    connector = Connector()
    """Establishes a secure connection to Cloud SQL using the Cloud SQL Python Connector."""
    return connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",  # No need to import pymysql explicitly
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )

def generate_avro_schema(table_name, column_names, types):
    """Generate Avro schema dynamically based on table structure"""

    avro_type_map = {
        "int": "int",
        "float": "float",
        "bool": "boolean",
        "str": "string",
        "bytes": "bytes",
    }

    # Generate schema fields
    fields = []
    for i in range(len(column_names)):
        avro_type = avro_type_map.get(types[i], types[i])
       
        # Handle datetime columns explicitly
        if "date" in types[i].lower() or "time" in types[i].lower():
            avro_type = {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        
        fields.append({"name": column_names[i], "type": ["null", avro_type], "default": None})

    # Return Avro schema
    return {
        "type": "record",
        "name": table_name,
        "fields": fields
    }

def backup_table_to_avro(table_name):
    """Backs up a MySQL table from Cloud SQL to Avro format and uploads it to GCS."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Fetch table data
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        if not rows:
            print(f"No data found in table {table_name}. Skipping backup.")
            return

        column_names = [desc[0] for desc in cursor.description]

        types = [type(item).__name__ for item in rows[0]]
        

        # Define Avro schema dynamically based on column names
        schema = generate_avro_schema(table_name, column_names, types)

        # Generate timestamped filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        avro_filename = f"{table_name}_backup_{timestamp}.avro"

        # Convert rows (tuples) into a list of dictionaries
        records = [dict(zip(column_names, row)) for row in rows]

        # Write to Avro
        with open(avro_filename, "wb") as avro_file:
            fastavro.writer(avro_file, schema, records) 

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

@functions_framework.http
def backup_function(response):
    """Backs up the tables"""
    tablenames = ['departments', 'jobs', 'hired_employees']
    for table in tablenames:
        print(f"Backup for table {table}")
        backup_table_to_avro(table)
    return {"message": "Tables backed up successfully"}