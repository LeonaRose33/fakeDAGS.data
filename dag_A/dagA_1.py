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

operateurs = "DIMOPERATEUR"


####################
## DAG DEFINITION ##
####################


with DAG(
    dag_id="Operateurs",
    catchup=False,
    tags=["CONSONEO", "ETL", "DIM"],
    schedule_interval="30 3 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="CET"),
) as dag:
    ######################
    ## TASKS DEFINITION ##
    ######################

    @task(task_id="Operateurs_Start")
    def operateur_start_task(dag_id):
        log.debug(f"{dag_id} started")

    @task(task_id="Operateurs_End")
    def operateur_end_task(dag_id, table):
        sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn_id)
        Utils.update_lastupdate(sf_hook, table)
        log.info(f"{dag_id} ended")

    @task(
        task_id="Operateurs",
        retries=3,
        retry_delay=timedelta(seconds=60),
    )
    def operateurs_task(mysql_conn_id, sf_conn_id, table):
        try:
            log.info(f"Retrieving Operateur data from dossier c2e database")
            mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
            with mysql_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    query = """(
                        select 	
                        CONCAT(UPPER(SUBSTRING(nom,1)), " ", UPPER(SUBSTRING(prenom,1,1)),LOWER(SUBSTRING(prenom,2))) NAME,
                        OU.id C2E_OPERATEURID,
                        CASE 	when OU.id in (501, 684, 629, 1106, 404, 439, 1315) then "CONSONEO - Conformité"
                                when OU.id in (437, 641, 756, 627) then "CONSONEO - Accompagnement Installateurs"
                                when OU.id in (1152, 1143, 1104, 902, 430, 1313) then "CONSONEO - B2C" 
                                ELSE UPPER(O.raison)
                        END EQUIPE, 
                        if (O.oblige_id, O.oblige_id, O.id) C2E_OBLIGEID,
                        if (O.oblige_id, O.id, 0) C2E_MANDATAIREID
                        FROM ObligeUser OU
                        LEFT JOIN Oblige O ON OU.oblige_id = O.id);
                        """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
            df = DataFrame(data=results, columns=columns)
            df = df[df["C2E_OBLIGEID"].notnull()]

            log.info(f"Retrieving oblige ids from analytics database")
            sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn_id)
            with sf_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    query = """SELECT ID OBLIGE_ID, C2E_OBLIGEID
                            FROM DIMOBLIGE"""
                    cursor.execute(query)
                    results = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
            df_oblige = DataFrame(data=results, columns=columns)

            log.info(f"Retrieving mandataire ids from analytics database")
            with sf_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    query = """SELECT ID MANDATAIRE_ID, C2E_OBLIGEID C2E_MANDATAIREID
                            FROM DIMMANDATAIRE
                            """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    columns = [col[0] for col in cursor.description]
            df_mandataire = DataFrame(data=results, columns=columns)

            df = (
                df.merge(df_oblige, on="C2E_OBLIGEID", how="left")
                .merge(df_mandataire, on="C2E_MANDATAIREID", how="left")
                .drop(["C2E_OBLIGEID", "C2E_MANDATAIREID"], axis=1)
            )
            df = df[df["OBLIGE_ID"].notnull()]
            df["NAME"].fillna("", inplace=True)

            log.info(f"Retrieving Operateurs data from analytics database")
            sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn_id)
            with sf_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    results = []
                    query = """SELECT 
                        ID, 
                        NAME NAME_DIM,
                        C2E_OPERATEURID,
                        EQUIPE EQUIPE_DIM
                    FROM DIMOPERATEUR"""
                    cursor.execute(query)
                    results = cursor.fetchall()
                    dim_columns = [col[0] for col in cursor.description]
            df_operateursdim = DataFrame(data=results, columns=dim_columns)

            df_insert = DataFrame()

            if df_operateursdim.empty:
                df_insert = df
                log.info(f"df_insert: {len(df_insert)}")

            else:
                df_final = df.merge(
                    df_operateursdim, on=["C2E_OPERATEURID"], how="left"
                )

                df_update = df_final[
                    ((~df_final.NAME_DIM.isna()) & (df_final.NAME != df_final.NAME_DIM))
                    | (
                        (~df_final.EQUIPE_DIM.isna())
                        & (df_final.EQUIPE != df_final.EQUIPE_DIM)
                    )
                ].drop(columns=["NAME_DIM", "EQUIPE_DIM"])
                if len(df_update) > 0:
                    log.info(f"lines to update: {len(df_update)})")
                    with sf_hook.get_conn() as conn:
                        with conn.cursor() as cursor:
                            rows = df_update.to_dict(orient="records")
                            counter = 0
                            for row in rows:
                                query = f"""update {table} 
                                            set NAME = {Utils.parsefield(row["NAME"])}, 
                                                EQUIPE = {Utils.parsefield(row["EQUIPE"])} 
                                            where C2E_OPERATEURID = {row["C2E_OPERATEURID"]};"""
                                cursor.execute(query)
                                conn.commit()   
                                counter += 1
                    log.info(f" {table} updated with {counter} rows")

                if len(df_final[df_final.NAME_DIM.isna()]) == 0:
                    log.info(f"{table} is up to date")

                else:
                    df_insert = df_final[df_final.NAME_DIM.isna()].drop(
                        columns=["NAME_DIM", "EQUIPE_DIM", "ID"]
                    )

            if not df_insert.empty:
                rows = df_insert.to_dict(orient="records")
                total = len(df_insert)
                counter = 0
                columns = df_insert.columns
                insert_columns = ", ".join(columns.insert(0, "ID"))
                insert_radical = (
                    f"""INSERT INTO {table}({insert_columns}) VALUES {{values}}"""
                )
                pending_values = []

                sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn_id)
                for row in rows:
                    values = ", ".join(
                        [f"'{uuid4().hex}'"]
                        + [Utils.parsefield(row[x]) for x in columns]
                    )
                    pending_values.append(f"({values})")
                    counter += 1
                    if (counter % 500) == 0:
                        log.debug(
                            f"{int(counter/total*100)}% lines inserted ({counter} / {total})."
                        )
                        Utils.bulk_insert_values(
                            sf_hook, insert_radical, pending_values
                        )
                        pending_values = []
                if pending_values != []:
                    Utils.bulk_insert_values(sf_hook, insert_radical, pending_values)
                    log.debug(
                        f"{int(counter/total*100)}% lines inserted ({counter} / {total})."
                    )

        except Exception as inst:
            log.error(traceback.format_exc())
            raise

    op_tsk = operateurs_task(
        mysql_conn_id=mysql_conn_id, sf_conn_id=sf_conn_id, table=operateurs
    )

    start = operateur_start_task(dag.dag_id)
    end = operateur_end_task(dag.dag_id, operateurs)

    start >> op_tsk >> end
