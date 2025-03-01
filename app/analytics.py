import pandas as pd

# Cloud SQL Connection Config
INSTANCE_CONNECTION_NAME = "hopeful-canto-452115-s3:us-central1:employee-db"
DB_USER = "db_admin"
DB_PASS = "password"
DB_NAME = "employees"

query1 = """
    SELECT 
        d.department, 
        j.job, 
        EXTRACT(QUARTER FROM he.datetime) AS quarter, 
        COUNT(*) AS num_hired
    FROM hired_employees he
    JOIN departments d ON he.department_id = d.id
    JOIN jobs j ON he.job_id = j.id
    WHERE EXTRACT(YEAR FROM he.datetime) = 2021
    GROUP BY d.department, j.job, quarter
    ORDER BY d.department, j.job, quarter;
"""

# Query 2: Departments that hired more than the mean number of employees in 2021
query2 = """
    WITH department_hires AS (
        SELECT 
            he.department_id, 
            d.department, 
            COUNT(*) AS num_hired
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        WHERE EXTRACT(YEAR FROM he.datetime) = 2021
        GROUP BY he.department_id, d.department
    )
    SELECT 
        department_id, 
        department, 
        num_hired
    FROM department_hires
    WHERE num_hired > (SELECT AVG(num_hired) FROM department_hires)
    ORDER BY num_hired DESC;
"""

def get_hired_employees_quarter(db):
    
    with db as conn:
        df1 = pd.read_sql(query1, conn)
    df1

    # Pivot the data to get the required format
    df_pivot = df1.pivot_table(index=["department", "job"], 
                            columns="quarter", 
                            values="num_hired", 
                            aggfunc="sum", 
                            fill_value=0)

    # Rename columns to match Q1, Q2, Q3, Q4
    df_pivot.columns = [f"Q{int(col)}" for col in df_pivot.columns]
    df_pivot.reset_index(inplace=True)
    json_string = df_pivot.to_json(orient="records")
    return json_string

def get_query2(db):

    with db as conn:
        df1 = pd.read_sql(query1, conn)
    
    json_string = df1.to_json(orient="records")
    return json_string