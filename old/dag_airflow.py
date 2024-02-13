from datetime import datetime, timedelta
import pandas as pd
import  logging 

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

# Code wrangling
# Env 


logger = logging.getLogger("airflow.task")

# 0 airflow installed already in the env cas_de

# 1 Set up the  User 
# airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

# 2 Start both webserver and scheduler
# airflow webserver -D
# airflow scheduler -D

# 3 make sure your script dags/ folder
# /Users/coding/airflow/dags/dag_airflow.py

# 4 Refresh airflow DB
# airflow db init

# 5 check current executor: SequentialExecutor
# airflow config get-value core executor

# 6 Check you dag is in the list
# http://localhost:8080/home

# 7 Run test like
# python dag_airflow.py


# 8 Run
# from the UI


# Airflow docs
# https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html


default_args = {
    'owner': 'data_wranglers',
    'start_date': datetime(2023, 12, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='loans_data_pipeline',
    default_args=default_args,
    description='Pipepline for wrangling loans data',
    schedule_interval=None,  
)

#templates_dict={
#"input_path": "/data/events/{{ds}}.json",
#"output_path": "/data/stats/{{ds}}.csv",},

# from airflow.sensors.filesystem import FileSensor
# wait_for_data = FileSensor(
#    task_id="wait_for_data",
#   filepath="/data/supermarket/data.csv",
#   mode="reschedule",
#)

def read_data():
    return pd.read_csv("/Users/coding/Documents/CAS_DE/cas-de-stat-vis/lc_loan_sample.csv",low_memory = False)


def persist_data(df):
    df.to_csv('/Users/coding/Documents/CAS_DE/cas-de-stat-vis/output_wrangling.csv', index=False, sep=';', encoding='utf-8')



def wrangling_pipeline():
    df = read_data()
    logging.info(f"""duplicates in dataframe :{df.duplicated().sum()}""") 
    df['emp_title'] = df['emp_title'].str.lower().str.strip()
    persist_data(df)   


start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

#wrangling_pipeline_task = PythonOperator(
wrangling_pipeline_task = PythonVirtualenvOperator(
    task_id='wrangling_pipeline',
    python_callable=wrangling_pipeline,
    provide_context=True,
    requirements=["pandas","urllib"],
    dag=dag,
)


end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


start_task >> wrangling_pipeline_task >> end_task

if __name__ == "__main__":
    dag.test()
