FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for pymssql
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    freetds-dev \
    freetds-bin \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=300 -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
