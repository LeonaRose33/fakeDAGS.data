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

from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator

DAG_NAME = "dagC_3"

with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 2},
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    section_1 = SubDagOperator(
        task_id="section-1",
        subdag=subdag(DAG_NAME, "section-1", dag.default_args),
    )

    some_other_task = EmptyOperator(
        task_id="some-other-task",
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> section_1 >> some_other_task >> end