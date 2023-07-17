from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import csv,os

default_args = {
    "owner": "srivatsan",
    "retries": 1,
    "retry_delay": timedelta(seconds=2)
}

#Queries for the last week's data and stores them in a csv file in the results directory
def query_table(**kwargs):

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

    ###TIP USE airflow MACROS only within task instances
    cursor.execute(open('dags/sql/query_week.sql',"r").read().format(kwargs['date']))

    try:
        os.mkdir("dags/results")
    except:
        pass

    with open(f"dags/results/{kwargs['date']}.csv","w") as f:
        csv_w = csv.writer(f)
        csv_w.writerow(i[0] for i in cursor.description)
        csv_w.writerows(cursor)
    print(os.path.realpath(f'{kwargs["date"]}.csv')) 
    conn.commit()
    cursor.close()
    conn.close()
    

with DAG(
    dag_id = 'weekly_query',
    default_args = default_args,
    start_date = datetime(2023,2,8),
    schedule_interval = '@weekly'
) as dag:

    #Query Operation using Hooks
    '''
    task1 = PythonOperator(
        task_id = 'query',
        python_callable=query_table,
        #Notice how we pass the macro as a kwargs instead of accessing it in Python Function directly
        op_kwargs={"date" : '{{ds}}'},
        email_on_failure = True
    )
    '''

    task1 = BashOperator(task_id = 'dbt_query',bash_command='''cd /postgres_dbt && dbt build --vars '{"today_date":"{{ds}}" }' ''')

    #Twilio Trigger
    task2 = TriggerDagRunOperator(
        task_id = 'Triggers_Twilio',
        trigger_dag_id= 'twilio_dag',
        trigger_rule = 'one_failed'
    )

    task1>>task2