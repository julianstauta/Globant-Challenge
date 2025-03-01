from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import engine, getconn
from app.schema import UploadDataSchema
from app.crud import insert_hired_employees, insert_departments, insert_jobs
from app.analytics import get_hired_employees_quarter

router = APIRouter()

@router.post("/upload-data")
async def upload_data(data: UploadDataSchema):
    db = Session(engine)
    try:
        if data.hired_employees:
            insert_hired_employees(db, data.hired_employees)
        if data.departments:
            insert_departments(db, data.departments)
        if data.jobs:
            insert_jobs(db, data.jobs)
        return {"message": "Data uploaded successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/get-hired-by-quarter")
async def get_hired_by_quarter():
    db = getconn
    try:
        return get_hired_employees_quarter(db)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))