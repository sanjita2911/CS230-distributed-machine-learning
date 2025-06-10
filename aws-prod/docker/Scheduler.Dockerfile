FROM python:3.9-slim

WORKDIR /app

# Install OS-level dependencies if needed (e.g., for pandas, scikit-learn)
RUN apt-get update && apt-get install -y build-essential

# Copy requirements and install Python dependencies
COPY docker/requirements_scheduler.txt .
RUN pip install -r requirements_scheduler.txt

# Copy scheduler application code
COPY scheduler .

# Environment variables (customize as needed)
ENV PYTHONUNBUFFERED=1

# Expose FastAPI port
EXPOSE 8000

# Run Uvicorn server for FastAPI app
CMD ["uvicorn", "scheduler:app", "--host", "0.0.0.0", "--port", "8000","--reload"]