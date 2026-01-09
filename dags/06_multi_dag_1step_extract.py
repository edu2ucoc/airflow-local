# TriggerDagRunOperator : 트리거를 직접 발동 시켜서 1번이 완료되면 2번 DAG을 실행시킴
from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심
import logging
import json
import random
import os

with DAG(
    dag_id              = "06_multi_dag_1step_extract",
    description         = "extract 전용 DAG",
    default_args        = {
        'owner'          :'de_1team_manager',
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['extract', 'etl']
) as dag:
    pass