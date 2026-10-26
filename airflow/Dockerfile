FROM apache/airflow:3.3.1

USER airflow

COPY --chown=airflow:root requirements.txt /tmp/project-requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    -r /tmp/project-requirements.txt \
    "dbt-core==1.11.12" \
    "dbt-postgres==1.10.2"
