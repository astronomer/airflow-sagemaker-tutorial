FROM quay.io/astronomer/astro-runtime:5.0.8

ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AWS_DEFAULT_REGION=us-east-2