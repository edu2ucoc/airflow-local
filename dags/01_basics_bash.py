# 기본 골격, bash 오퍼레이터 적용
# airflow 상에서 dag가 어떻게 작동하는지 기초
# timedelta : 시간의 차이를 계산하는 함수
from datetime import datetime, timedelta 
# DAG
from airflow import DAG
# 오퍼레이터
from airflow.operators.bash import BashOprator

# 1. 기본 인자 구성 -> task에 적용될 매개변수
#    dag 소유주(작성자, 관리자), 
#    과거 데이터 누락분에 대한 소급 실행 여부, 
#    작업 실패(빈번하게(I/o일 확률이 높다) 발생될수 있음)가 발생했을때 재시도 여부->1회만등등 설정
#    실패후 다시 시도할때 텀(간격) 설정. 몇분후 다시 시도
default_args = {
    'owner'          :'de_1team_manager', # DAG 주인
    'depends_on_past': False,             # 과거 데이터 소급 처리 금지
    'retries'        : 1,                 # 작업 실패시 재시도는 1회 자동 진행
    'retry_delay'    : timedelta(minutes=5) # 실패 시 5분 후에 재시도
    # 시나리오 => 작업 성공 => 완료
    # 시나리오 => 작업 실패 => 5분후 => 1회 재시도 => 성공 => 완료
    # 시나리오 => 작업 실패 => 5분후 => 1회 재시도 => 실패 => 해당 시점의 작업 포기(소급 방지)
}

# 2. DAG 정의
