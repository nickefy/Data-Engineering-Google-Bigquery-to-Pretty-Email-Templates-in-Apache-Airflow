from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowException
from airflow import models
from airflow import DAG
from operators import Operator
from datetime import datetime, timedelta
import os

# dag run interval
schedule_interval_dag = timedelta(days=1)

# query to extract data
query = """
SELECT * from sample_table where date = $EXECUTION_DATE
        """
#receivers
receivers = 'youremail@gmail.com'

#theme for html table
table_theme = 'blue_light'

# official start date of the pipeline
start_time = datetime(2021, 3, 7, 10)

default_dag_args = {
    'start_date': start_time,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'on_failure_callback': notify_email,
    'retry_delay': timedelta(minutes=5),
}

with models.DAG(
    dag_id= 'your DAG name',
    schedule_interval = schedule_interval_dag,
    catchup = True,
    default_args=default_dag_args) as dag:

    bigquery_to_email = Operator.BigqueryToEmail(
        task_id = 'Extract Data and Send Email',
        query = query,
        receivers = receivers,
        table_theme = table_theme
        )

    bigquery_to_email
