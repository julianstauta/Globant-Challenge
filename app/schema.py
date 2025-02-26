from pydantic import BaseModel
from datetime import datetime
from typing import List

class HiredEmployeeSchema(BaseModel):
    id: int
    name: str
    datetime: datetime
    department_id: int
    job_id: int

class DepartmentSchema(BaseModel):
    id: int
    department: str

class JobSchema(BaseModel):
    id: int
    job: str

class UploadDataSchema(BaseModel):
    hired_employees: List[HiredEmployeeSchema] = []
    departments: List[DepartmentSchema] = []
    jobs: List[JobSchema] = []
