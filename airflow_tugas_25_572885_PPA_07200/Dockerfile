FROM apache/airflow:2.10.5-python3.12

USER airflow
RUN pip install --no-cache-dir pandas matplotlib \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
