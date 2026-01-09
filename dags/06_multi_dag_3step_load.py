from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # 핵심
import logging
import os
import pandas as pd
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

with DAG(
    dag_id              = "06_multi_dag_3step_load",
    description         = "load 전용 DAG",
    default_args        = {
        'owner'          :'de_1team_manager',
    },    
    schedule_interval   = '@daily',
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['load', 'etl']    
) as dag:
    pass
