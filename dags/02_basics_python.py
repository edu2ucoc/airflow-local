# 파이썬 로직을 task로 사용
# task간 통신을 통해서 상호 대화

from datetime import datetime, timedelta 
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging # 레벨별로 로그 출력 가능 (에러, 경고, 디버깅, 정보 등등 레벨 지정)

with DAG(
    dag_id              = "02_basics_python_v1",
    description         = "파이썬 task 구성 및 통신(xcom)",
    default_args        = {
        'owner'          :'de_1team_manager',        
        'retries'        : 1,
        'retry_delay'    : timedelta(minutes=1)
    },
    schedule_interval   = '@once', # 수동으로 딱 한번 수행, 주기성 없음
    start_date          = datetime(2025,1,1),
    catchup             = False,
    tags                = ['python','xcom', 'context']
) as dag:
    # 1. task 정의 (ETL을 고려하여서 네이밍)
    extract_task   = PythonOperator()
    transform_task = PythonOperator()
    # 2. 의존성 (순서 작성)
    extract_task >> transform_task
    # [실습] : load_task 구현하시오. 의존성은 transform_task 실행 성공후 task 수행되게 구성
    pass