# Use official Python base image
FROM python:3.9

# Copy the requirements t file
COPY requirements.txt ./

# Set working directory
WORKDIR /app

# Copy the application files
COPY main.py requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8080

# Run FastAPI using Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]