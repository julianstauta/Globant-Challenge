from fastapi import FastAPI
from app.routes import router

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, Cloud Run!"}

app.include_router(router)