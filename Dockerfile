FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

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

CMD ["gunicorn", "app.main:app", \
     "--workers", "4", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "120", \
     "--graceful-timeout", "30", \
     "--keep-alive", "5"]
