# Airflow image extended with the equity pipeline's runtime dependencies so the
# DAG can import and run the pipeline modules directly.
FROM apache/airflow:2.10.5

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
