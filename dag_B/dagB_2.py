import logging
import sys
import traceback
from datetime import datetime, timedelta
from hashlib import sha256

import pendulum
import pytz
from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.task_group import TaskGroup
from pandas import DataFrame, isna, merge
import numpy as np

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

log = logging.getLogger(__name__)
sout_handler = logging.StreamHandler(sys.stdout)
sout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
log.addHandler(sout_handler)

################
## PARAMETRES ##
################

sf_conn_id = "analytics"
mysql_conn_id = "dossierc2e"

####################
## DAG DEFINITION ##
####################

with DAG(
    dag_id="dagB_2",
    catchup=False,
    tags=["LEONA", "TEST"],
    start_date=datetime(2022, 1, 1)
) as dag:

    @task(task_id="task1")
    def task1():
        
        log.info("starting task 1")

    task1 = task1()

    @task(task_id="task2")
    def task2():
        log.info("starting task 2")

    task2 = task2()

    @task(task_id="task3")
    def task3():
        log.info("starting task 3")

    task3 = task3()

    ######################
    ## TASKS DEFINITION ##
    ######################