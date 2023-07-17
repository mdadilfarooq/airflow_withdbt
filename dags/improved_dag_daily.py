from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
import requests
import pandas as pd
import geocoder



default_args = {
    "owner": "srivatsan",
    "retries": 1,
    "retry_delay": timedelta(seconds=2),
    'email':"neverafkst@gmail.com"
}

API_DAILY = "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&daily=weathercode,temperature_2m_max,temperature_2m_min,sunrise,sunset,precipitation_sum&timezone=auto"


#Runs API and gets the json
#Parses it and creates commands to add it to the table
#Better than SimpleHttpOperator as it allowes to give multiple requests in a loop

def run_apis(ti):
    rd = pd.read_csv('dags/csv/cities.csv')
    end_string = "INSERT INTO weather_data (city,latitude,longitude,weather_date,weather_code,temperature_max_c,temperature_min_c,sunrise,sunset,precipitation) VALUES"
    cities = []
    date_of_query = 0
    for row in rd.index:
        #geocoder library which was extended into the AIRFLOW docker image 
        geo = geocoder.arcgis(rd['City'][row])
        
        city = str(rd['City'][row])
        cities.append(city)
        lat = geo.latlng[0]
        long = geo.latlng[1]

        ###API call using requests library
        res = requests.get(API_DAILY.format(lat,long))
        res = json.loads(res.text)

        date_of_query = str(res['daily']['time'][0])
        code = str(res['daily']['weathercode'][0])
        temp_max =  str(res['daily']['temperature_2m_max'][0])
        temp_min = str(res['daily']['temperature_2m_min'][0])
        sunrise = str(res['daily']['sunrise'][0])
        sunset = str(res['daily']['sunset'][0])
        prec =  str(res['daily']['precipitation_sum'][0])
       
        end_string += "('{}',{},{},to_date('{}','YYYY-MM-DD'),{},{},{},'{}'::timestamptz,'{}'::timestamptz,{}),".format(city,lat,long,date_of_query,code,temp_max,temp_min,sunrise,sunset,prec)

    ##XCOMS Push in Task - run_apis
    ti.xcom_push(key = 'date',value = date_of_query)
    ti.xcom_push(key = 'cities',value = city)

    end_string = end_string[:-1]
    return end_string


#Pulls from the XCOM the commands to insert to table
#Checks for table existance and creates if not present
#Checks for duplicates and inserts into the table in one connection
def create_table_insert(ti,**kwargs):

    date = ti.xcom_pull(task_ids = 'run_apis',key = 'date')
    cities = ti.xcom_pull(task_ids = 'run_apis',key = 'cities')
    #Converting cities tuple to string so as to use it in query
    cities = str(tuple(cities))
    sel_sql = "SELECT * FROM weather_data where weather_date = '{}' and city not in {}".format(date,cities)

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(open("dags/sql/create_weather_table.sql","r").read())
    cursor.execute(sel_sql)
    res = cursor.fetchall()
    if not res:
        cursor.execute(kwargs['sql'])
    else:
        print(res)

    conn.commit()
    cursor.close()
    conn.close()


#DAG initialization with default arguments and starting date,schedule
with DAG(
    dag_id = 'project_dag',
    default_args = default_args,
    start_date = datetime(2023,2,8),
    schedule_interval = '@daily'
) as dag:

    #Run APIs on the and process the JSON to be added to the table
    task1 = PythonOperator(
        task_id = 'run_apis',
        python_callable= run_apis,
        email_on_failure = True
    )

    #Creates and writes to the table
    task2 = PythonOperator(
        task_id = 'write_to_table',
        python_callable=create_table_insert,
        op_kwargs = {'sql':'{{ti.xcom_pull(task_ids = "run_apis",key = "return_value")}}'},
        email_on_failure = True
    )

    #Twilio Trigger
    task3 = TriggerDagRunOperator(
        task_id = 'Triggers_Twilio',
        trigger_dag_id= 'twilio_dag',
        trigger_rule = 'one_failed'
    )

    task1>>task2>>task3