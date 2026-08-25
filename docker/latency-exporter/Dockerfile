FROM python:3.11-slim
RUN pip install --no-cache-dir kafka-python==2.0.2 prometheus_client==0.20.0 \
    pyarrow==17.0.0 boto3==1.34.0
COPY exporter.py /app/exporter.py
WORKDIR /app
EXPOSE 8000
ENTRYPOINT ["python", "exporter.py"]
