from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

# Database connection setup
DATABASE_URL = "mysql+pymysql://user:password@/cloudsql/your-instance-ip/dbname"
engine = sqlalchemy.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

app = FastAPI()

# Pydantic Models
class HiredEmployee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

    def validate(self):
        try:
            datetime.strptime(self.datetime, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            raise ValueError("Invalid datetime format")

class Department(BaseModel):
    id: int
    department: str

class Job(BaseModel):
    id: int
    job: str

class DataPayload(BaseModel):
    hired_employees: Optional[List[HiredEmployee]] = Field(default=[])
    departments: Optional[List[Department]] = Field(default=[])
    jobs: Optional[List[Job]] = Field(default=[])

@app.post("/upload")
def upload_data(payload: DataPayload):
    session = SessionLocal()
    
    try:
        # Insert Departments
        if payload.departments:
            session.bulk_insert_mappings(Department, [d.dict() for d in payload.departments])

        # Insert Jobs
        if payload.jobs:
            session.bulk_insert_mappings(Job, [j.dict() for j in payload.jobs])

        # Validate Hired Employees and Insert
        if payload.hired_employees:
            for emp in payload.hired_employees:
                emp.validate()
            session.bulk_insert_mappings(HiredEmployee, [e.dict() for e in payload.hired_employees])

        session.commit()
        return {"message": "Data uploaded successfully"}
    
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    
    finally:
        session.close()