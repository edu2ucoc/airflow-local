from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
# mysql
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

# 데이터
import json
import random
import pandas as pd
import os

# 프로젝트 폴더 내부에 /dags/data에 내부에서 TASK 과정에 생성되는 파일을 동기화하여 확인할수 있게
# 위치를 선정
DATA_PATH = '/opt/airflow/dags/data' # 도커 내부에 있는 서비스(리눅스기반)의 특정경로
os.makedirs( DATA_PATH , exist_ok=True)

# DAG
with DAG(
    dag_id              = "05_mysql_etl_v1",
    description         = "etl 수행하여 mysql에 적제",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['mysql', 'etl']
) as dag:
    # 오퍼레이터(task)
    create_table    = MySqlOperator()
    extract_data    = PythonOperator()
    transform_data  = PythonOperator()
    load_data       = PythonOperator()

    # 의존성
    # 테이블 생성 >> Extract >> transform >> Load
    create_table >> extract_data >> transform_data >> load_data