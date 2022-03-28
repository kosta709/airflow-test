from airflow import DAG
from datetime import timedelta
from datetime import datetime

import os

from airflow.operators.dummy_operator import DummyOperator

ROOT_PATH = 'dags.' if os.environ.get('IS_LOCAL', False) else ''
module = __import__(f'{ROOT_PATH}utils.utils', fromlist=[
                                                        'long_task',
                                                    ]
                    )
long_task = module.long_task
start_date = datetime(2000, 1, 1) if os.environ.get('IS_LOCAL', False) else datetime(2021, 3, 5, 8, 20, 0)

default_args = {
    "owner": "admin",
    "start_date": start_date,
    "email": ["admin@dipcoding.ai"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
    "catchup_by_default": False,
    "countdown": 3,
    "catchup": False
}

schedule_interval = None
dag = DAG(
    "Test-Sigterm-issue",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
)

start = DummyOperator(task_id=f'start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

downstream_task = long_task("downstream_task", 10, dag)
start >>  [long_task("p1", 10, dag),
           long_task("p2", 1, dag) ,
           long_task("p3", 2, dag) ,
           long_task("p4", 4, dag) ,
           long_task("p5", 3, dag) ,
           long_task("p6", 5, dag) ] >> downstream_task >> end

dag_error = DAG(
    "Test-Error",
    default_args=default_args,
    schedule_interval=schedule_interval,
    max_active_runs=1,
)
start_e = DummyOperator(task_id=f'start', dag=dag_error)
end_e = DummyOperator(task_id='end', dag=dag_error)
downstream_task_e = long_task("downstream_task", 10, dag_error)
start_e >>  [long_task("p1", 10, dag_error),
           long_task("p2", 1, dag_error) ,
           long_task("p3-error", 2, dag_error) ,
           long_task("p4", 4, dag_error) ,
           long_task("p5", 3, dag_error) ,
           long_task("p6", 5, dag_error) ] >> downstream_task_e >> end_e

