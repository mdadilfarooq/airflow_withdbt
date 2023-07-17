from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

with DAG(
    dga_id = "DAG_context", 
    start_date=datetime.datetime(2022, 2, 10, tz="UTC"),
    schedule="0 * * * *", 
    catchup=False
) as dag:

    task1 = EmptyOperator(task_id="task1",depends_on_past='True')

    @task(task_id = 'task2')
    def task_2():
        pass

    @task.branch(task_id = 'task3')
    def task_3(a):
        if a%2:
            return 'task4'
        else:
            return None
        
    task4 = EmptyOperator(task_id="task4")

    task5 = EmptyOperator(task_id = 'task5',trigger_rule= 'one_failure')

    [task1,task4]>>task5

