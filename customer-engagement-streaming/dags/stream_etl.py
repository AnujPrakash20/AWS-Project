from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka_utils.kafka_stream import create_kafka_stream
from datetime import datetime


default_args = {
    "owner": "Anuj Prakash",
    "start_date": datetime(2025,7,19)
}

# DAG Definition

dag = DAG(
    dag_id = "click_stream_pipeline",
    default_args = default_args,
    schedule = "@daily",
    catchup = False,
    tags = ["clickstream","kafka"]
)

# Task Definition

kafka_stream = PythonOperator(
    task_id = "kafka_stream",
    python_callable = create_kafka_stream,
    dag = dag
)

kafka_stream