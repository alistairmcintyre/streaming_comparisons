# Lean image for the CDC data generators (Postgres INSERT/UPDATE/DELETE → Debezium).
# Decoupled from the deprecated streamlit image, the generator only needs psycopg2.
# The generator source is volume-mounted (./generators) and run via the compose
# entrypoint, so this image just provides Python + the Postgres driver.
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
ENV PYTHONUNBUFFERED=1
