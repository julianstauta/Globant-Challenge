from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import engine
from app.schemas import UploadDataSchema
from app.crud import insert_hired_employees, insert_departments, insert_jobs

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
