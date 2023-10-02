from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
import pandas as pd
import os
import sys

parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_folder)

from python_script.APIToCsv import main
from python_script.sparkProcess import connection, process_data

def APIToDB():

    dest_hook = PostgresHook(postgres_conn_id="postgres_conn") 

    df , create_sql_script = main()

    conn = dest_hook.get_conn()
    cur = conn.cursor()
    cur.execute(create_sql_script)
    conn.commit()
    cur.close()

    rows = list(df.itertuples(index=False , name=None))
    dest_hook.insert_rows(table = "staging.example", rows = rows)

def DBToDB():
    dest_hook = PostgresHook(postgres_conn_id="postgres_conn") 

    df = dest_hook.get_pandas_df(sql=f"select * from staging.example")

    spark = connection()

    df = process_data(spark=spark, df=df)

    rows = list(df.itertuples(index=False , name=None))
    dest_hook.insert_rows(table = "public.example", rows = rows)


    


with DAG(
    dag_id = "APIToDBDags",
    schedule_interval="@daily",
    start_date=days_ago(1),
    tags=['dags'],
) as dag:


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')  
        
    APIToDB_task = PythonOperator(
        task_id=f"APIToDBTask",
        provide_context=True,
        python_callable = APIToDB        
    )

    DBToDBTask = PythonOperator(
        task_id=f"DBToDBTask",
        provide_context=True,
        python_callable = DBToDB        
    )
    
    start >> APIToDB_task >> DBToDBTask >> end