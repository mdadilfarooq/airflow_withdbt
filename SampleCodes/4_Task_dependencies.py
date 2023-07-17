from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default = {
    "owner": "srivatsan",
    "retries": 1,
    "retry_delay": timedelta(seconds=2)
}

with DAG(
    dga_id = "DAG_context", 
    start_date=datetime.datetime(2022, 2, 10, tz="UTC"),
    schedule="0 * * * *", 
    catchup=False,
    default_args=default
) as dag:

    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    task3 = EmptyOperator(task_id="task3")

    #task3 triggered when all it's upstream tasks are successes
    [task1,task2] >> task3
    
    #above is same as
    #task1.set_downstream(task3)
    #task2.set_downstream(task3)

