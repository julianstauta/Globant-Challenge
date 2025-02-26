from sqlalchemy.orm import Session
from app.models import HiredEmployee, Department, Job
from app.schema import HiredEmployeeSchema, DepartmentSchema, JobSchema

def insert_hired_employees(db: Session, employees: list[HiredEmployeeSchema]):
    try:
        for emp in employees:
            db.add(HiredEmployee(**emp.dict()))
        db.commit()
        return {"message": f"{len(employees)} employees inserted"}
    except Exception as e:
        db.rollback()
        raise e

def insert_departments(db: Session, departments: list[DepartmentSchema]):
    try:
        for dep in departments:
            db.add(Department(**dep.dict()))
        db.commit()
        return {"message": f"{len(departments)} departments inserted"}
    except Exception as e:
        db.rollback()
        raise e

def insert_jobs(db: Session, jobs: list[JobSchema]):
    try:
        for job in jobs:
            db.add(Job(**job.dict()))
        db.commit()
        return {"message": f"{len(jobs)} jobs inserted"}
    except Exception as e:
        db.rollback()
        raise e
