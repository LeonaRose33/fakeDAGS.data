from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)

def controller ():
    @task
    def start():
        print('Start')

    trigger = TriggerDagRunOperator(
        task_id='trigger_task_dag',
        trigger_dag_id='dagA_2',
        conf={'task1': 'data'}
        wait_for_completion = True
        )

    @task2
    def end ():
        print('Done')