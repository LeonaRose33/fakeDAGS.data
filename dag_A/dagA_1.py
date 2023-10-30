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

from utils import Utils
from uuid import uuid4

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

log = logging.getLogger(__name__)
sout_handler = logging.StreamHandler(sys.stdout)
sout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
log.addHandler(sout_handler)

sf_conn_id = "analytics"
mysql_conn_id = "dossierc2e"

####################
## DAG DEFINITION ##
####################

with DAG(
    dag_id="dagA_1",
    catchup=False,
    tags=["CONSONEO", "ETL"],
) as dag:
    
    
    


    ######################
    ## TASKS DEFINITION ##
    ######################
