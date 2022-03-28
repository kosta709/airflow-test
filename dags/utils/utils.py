from datetime import timedelta
from datetime import datetime


import os, time

# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def log(message):
    print(message)

def do_something(task_id, duration_seconds):
    #task_id = kwargs["task_id"][0], 
    #   duration_seconds = kwargs["duration_seconds"][0],
    log(f"Starting task_id={task_id} - duration_seconds={duration_seconds}")
    if "error" in task_id:
         log(f"Error task_id={task_id}")
         raise Exception("TEST Exception - task name contains error")
        
    time.sleep(duration_seconds)
    log(f"Finish task_id={task_id} - duration_seconds={duration_seconds}")

def long_task(task_name, duration_seconds, dag):
    
    #task_id = f"{task_name}-{duration_seconds}s"
    task_id = f"{task_name}"
    _task = PythonOperator(
            task_id=task_id,
            # provide_context=False,
            python_callable=do_something,
            op_kwargs={
                'task_id': task_id,
                'duration_seconds': duration_seconds
            },
            dag=dag)
    return _task
