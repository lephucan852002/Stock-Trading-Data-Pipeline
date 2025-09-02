FROM python:3.11-slim

WORKDIR /app

# system deps for oracledb - USE THIN MODE (no libaio needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy engine sources
COPY . .

# default: run matching engine. You can override CMD in docker-compose to run risk/settlement
CMD ["python", "matching_engine.py"]
