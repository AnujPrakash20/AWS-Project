import os
import sys

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipelines import reddit_pipeline


default_args = {
    'owner': 'Anuj prakash',
    'start_date': datetime(2025,5,29)
}

file_postfix = datetime.now().strftime("%Y%m%d")


#DAG Definition
dag = DAG(
    dag_id = 'etl_reddit_pipeline',
    default_args = default_args, #Default arguments that apply to all tasks unless overridden.
    schedule = '@daily',
    catchup = False, #False means Airflow will not try to run missed historical jobs from the past.
    tags = ['reddit','etl','pipeline'] #Tags are metadata to help filter DAGs in the Airflow UI.
)

#Task Definition 

#Extraction From Reddit
extract = PythonOperator(
    task_id = 'reddit_extraction',
    python_callable = reddit_pipeline,
    op_kwargs = {
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

# At execution time, Airflow will run the pipeline with the above keyword arguments:
# reddit_pipeline(
#     file_name='reddit_<your_file_postfix>',
#     subreddit='dataengineering',
#     time_filter='day',
#     limit=100
# )


 # Upload to s3 bucket

upload_s3 = PythonOperator(
    task_id = 's3_upload',
    python_callable = upload_s3_pipeline,
    dag=dag
)
 
extract >> upload_s3

# extract