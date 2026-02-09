# Data Engineer Test - ETL pipeline (SQL Server client)
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY data/ ./data/
COPY sql/ ./sql/
COPY .env.example .env.example

ENV PROJECT_ROOT=/app

CMD ["python", "-m", "src.pipeline.orchestrator"]
