import functions_framework
import fastavro
from google.cloud.sql.connector import Connector
from google.cloud import storage
import os
import tempfile
import datetime

# Cloud SQL Config
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")  # Storage bucket where backups are stored

TABLE_SCHEMAS = {
    "hired_employees": """
        CREATE TABLE hired_employees (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            datetime TIMESTAMP NOT NULL,
            department_id INTEGER REFERENCES departments(id),
            job_id INTEGER REFERENCES jobs(id)
        );
    """,
    "departments": """
        CREATE TABLE departments (
            id SERIAL PRIMARY KEY,
            department VARCHAR(255) UNIQUE NOT NULL
        );
    """,
    "jobs": """
        CREATE TABLE jobs (
            id SERIAL PRIMARY KEY,
            job VARCHAR(255) UNIQUE NOT NULL
        );
    """
}

# GCS Client
storage_client = storage.Client()


def get_db_connection():
    """Creates a secure connection to Cloud SQL."""
    connector = Connector()
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

def get_latest_backup_file(bucket_name, table_name):
    """Finds the latest Avro backup file for the given table in GCS."""
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"backup/{table_name}_backup_"))

    # Extract timestamps and sort files by newest
    avro_files = [
        (blob.name, datetime.datetime.strptime("_".join(blob.name.split("_")[-2:]).replace(".avro", ""), "%Y%m%d_%H%M%S"))
        for blob in blobs
        if blob.name.endswith(".avro")
    ]

    if not avro_files:
        raise FileNotFoundError(f"No backup files found for table {table_name}")

    # Get latest file
    latest_file = max(avro_files, key=lambda x: x[1])[0]
    latest_file = latest_file.replace('backup/', '')
    print(f"Using latest backup file: {latest_file}")
    return latest_file


def convert_avro_data(record, schema):
    """Converts Avro timestamps (timestamp-micros) to Python date objects."""
    for field in schema["fields"]:
        field_name = field["name"]
        field_type = field["type"]

        # Check if it's a timestamp field
        if isinstance(field_type, dict) and field_type.get("logicalType") == "timestamp-micros":
            timestamp_micros = record.get(field_name)
            if timestamp_micros is not None:
                # Convert microseconds to datetime.date
                record[field_name] = datetime.datetime.fromtimestamp(timestamp_micros / 1_000_000).date()

    return record

def drop_and_recreate_table(connection, table_name):
    """Drops and recreates a table before restoring data."""
    with connection.cursor() as cursor:
        print(f"Dropping table {table_name} if exists...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print(f"Recreating table {table_name}...")
        cursor.execute(TABLE_SCHEMAS[table_name])
    connection.commit()

def restore_table_from_avro(bucket_name, file_name, table_name):
    """Restores a Cloud SQL table from an Avro backup file stored in GCS."""
    try:
        print(f"Starting restore for table {table_name} from file {file_name}")

        # Download Avro file
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"backup/{file_name}")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            blob.download_to_filename(temp_file.name)
            avro_path = temp_file.name

        # Read Avro file
        with open(avro_path, "rb") as avro_file:
            reader = fastavro.reader(avro_file)
            schema = reader.writer_schema
            records = [convert_avro_data(record, schema) for record in reader]  # Convert timestamps

        # Extract column names from schema
        columns = [field["name"] for field in schema["fields"]]
        placeholders = ", ".join(["%s"] * len(columns))
        column_names = ", ".join(columns)

        # Insert data into Cloud SQL
        conn = get_db_connection()
        cursor = conn.cursor()

        drop_and_recreate_table(conn, table_name)

        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        batch_size = 1000

        # Insert in batches
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            values = [tuple(record.get(col, None) for col in columns) for record in batch]
            cursor.executemany(insert_query, values)
            conn.commit()

        print(f"Successfully restored {len(records)} rows into {table_name}")

        return {"status": "success", "message": f"Restored {len(records)} rows into {table_name}"}

    except Exception as e:
        print(f"Error restoring {file_name}: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()


@functions_framework.http
def restore_function(request):
    """Cloud Function Triggered via HTTP to restore a table from a backup file."""
    request_json = request.get_json(silent=True)
    
    if not request_json or "table_name" not in request_json:
        return {"status": "error", "message": "Missing required parameter 'table_name'"}, 400

    table_name = request_json["table_name"]
    backup_file = request_json.get("backupfile")
    
    if not backup_file or backup_file == "":
        try:
            backup_file = get_latest_backup_file(BUCKET_NAME, table_name)
        except FileNotFoundError as e:
            return {"status": "error", "message": str(e)}, 404

    result = restore_table_from_avro(BUCKET_NAME, backup_file, table_name)
    return result, 200 if result["status"] == "success" else 500