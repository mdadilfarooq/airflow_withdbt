from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag(start_date=datetime.datetime(2022,2, 10, tz="UTC"),
     schedule="@daily", catchup=False)
def generate_dag():
    op = EmptyOperator(task_id="task")

dag = generate_dag()