from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator


my_dag = DAG(   dag_id="DAG_constructor", 
                start_date=datetime.datetime(2022, 2, 10, tz="UTC"),
                schedule="@daily", catchup=False)

op = EmptyOperator(task_id="task", dag=my_dag)