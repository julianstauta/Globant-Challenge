import os
import pandas as pd
import pyarrow as pa
import pyarrow.avro as pavro
from google.cloud import storage
import mysql.connector
from datetime import datetime

# Environment Variables (Set in Cloud Run / Function)
CLOUD_SQL_HOST = os.getenv("CLOUD_SQL_HOST")
CLOUD_SQL_USER = os.getenv("CLOUD_SQL_USER")
CLOUD_SQL_PASSWORD = os.getenv("CLOUD_SQL_PASSWORD")
CLOUD_SQL_DATABASE = os.getenv("CLOUD_SQL_DATABASE")
GCS_BUCKET = os.getenv("GCS_BUCKET")

# Initialize Storage Client
storage_client = storage.Client()

def backup_table_to_avro(table_name):
    """Extracts table data from Cloud SQL and saves as AVRO in GCS."""
    try:
        # Connect to Cloud SQL
        conn = mysql.connector.connect(
            host=CLOUD_SQL_HOST,
            user=CLOUD_SQL_USER,
            password=CLOUD_SQL_PASSWORD,
            database=CLOUD_SQL_DATABASE
        )
        cursor = conn.cursor()

        # Fetch table data
        query = f"SELECT * FROM {table_name};"
        cursor.execute(query)
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        # Convert to Pandas DataFrame
        df = pd.DataFrame(rows, columns=col_names)

        # Convert DataFrame to Apache Arrow Table
        table = pa.Table.from_pandas(df)

        # Define AVRO file path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        avro_filename = f"{table_name}_{timestamp}.avro"
        local_path = f"/tmp/{avro_filename}"

        # Save as AVRO
        with pa.OSFile(local_path, "wb") as f:
            pavro.write_table(table, f)

        # Upload to GCS
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"backups/{avro_filename}")
        blob.upload_from_filename(local_path)

        print(f"Backup completed: gs://{GCS_BUCKET}/backups/{avro_filename}")

    except Exception as e:
        print(f"Error backing up {table_name}: {e}")

    finally:
        cursor.close()
        conn.close()

# Example usage
if __name__ == "__main__":
    tables = ["hired_employees", "departments", "jobs"]  # List of tables to back up
    for table in tables:
        backup_table_to_avro(table)
