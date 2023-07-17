from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "srivatsan",
    "retries": 1,
    "retry_delay": timedelta(seconds=2),
    'email':"neverafkst@gmail.com"
}


def twilio():
    #Perform Twilio Operations here
    pass


with DAG(
    dag_id = 'twilio_dag',
    default_args = default_args,
    start_date = datetime(2023,2,8),
    schedule_interval = None
) as dag:  

    task = PythonOperator(
        task_id = 'twilio_call',
        python_callable=twilio
    )