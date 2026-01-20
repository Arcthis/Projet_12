FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ .
COPY test_unified_pipeline.py .

VOLUME /app/logs

EXPOSE 8050

CMD ["python", "main.py"]
