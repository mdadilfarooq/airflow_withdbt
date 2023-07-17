from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id = "DAG_context", 
    start_date=datetime.datetime(2022, 2, 10, tz="UTC"),
    schedule="@daily", 
    catchup=False
) as dag:
    op = EmptyOperator(task_id="task")
    op2 = EmptyOperator()
