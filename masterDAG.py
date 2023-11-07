from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

start_date = datetime(year=2023, month=11, day=6)
current_date = datetime.now()

@dag(dag_id = 'master', start_date=start_date, schedule='@daily', catchup=False)


def master ():
    
    @task
    def start():
        print('Start')
        print(start_date)
        print(current_date)

    trigger_STATIC = TriggerDagRunOperator(
        task_id='trigger_STATIC',
        trigger_dag_id='dagA_3',
        )
    
    trigger_REDIS = TriggerDagRunOperator(
        task_id='trigger_REDIS',
        trigger_dag_id='dagC_3',
        )
        
    with TaskGroup('DIMS') as DIMS:
        trigger_document = TriggerDagRunOperator(
            task_id='trigger_document',
            trigger_dag_id='dagA_2',
            )
        
        trigger_operation = TriggerDagRunOperator(
            task_id='trigger_operation',
            trigger_dag_id='dagB_2',
            )
        trigger_organisation = TriggerDagRunOperator(
            task_id='trigger_organisation',
            trigger_dag_id='dagC_1',
            )
        trigger_operarteur = TriggerDagRunOperator(
            task_id='trigger_operateurs',
            trigger_dag_id='dagC_2',
            )
        
        trigger_organisation >> trigger_operarteur


    with TaskGroup('FACTS') as FACTS:
        trigger_controloperateurs = TriggerDagRunOperator(
            task_id='trigger_controloperateurs',
            trigger_dag_id='dagA_1',
            )
        
        trigger_factagg = TriggerDagRunOperator(
            task_id='trigger_factagg',
            trigger_dag_id='dagB_1',
            )
        
        trigger_factrecord = TriggerDagRunOperator(
            task_id='trigger_factrecord',
            trigger_dag_id='dagE_1',
            )
        
        trigger_factprediction = TriggerDagRunOperator(
            task_id='trigger_factpredicition',
            trigger_dag_id='dagD_1',
            )

    start = start()

    with TaskGroup('VIZ_DEMO') as VIZ_DEMO:
                
        with TaskGroup('grp_1') as grp_1:
            trigger_RECORDVIZ = TriggerDagRunOperator(
                            task_id='trigger_RECORDVIZ',
                            trigger_dag_id='dagE_2',
                            wait_for_completion = True
                            )
            trigger_DEMORECORDVIZ= TriggerDagRunOperator(
                    task_id='trigger_DEMORECORDVIZ',
                            trigger_dag_id='dagD_2',
                            wait_for_completion = True
                            )
            trigger_STOCKVIZ= TriggerDagRunOperator(
                    task_id='trigger_STOCKVIZ',
                            trigger_dag_id='dagD_2',
                            wait_for_completion = True
                            )
            trigger_FACTPREDICTION= TriggerDagRunOperator(
                    task_id='trigger_FACTPREDICTION',
                            trigger_dag_id='dagD_2',
                            wait_for_completion = True
                            )
            
            with TaskGroup('EJDEMOSTOCK') as EJDEMOSTOCK:
                trigger_DEMOSTOCKVIZ= TriggerDagRunOperator(
                        task_id='trigger_DEMO_STOCKVIZ',
                                trigger_dag_id='dagD_2',
                                wait_for_completion = True
                                )
                
                if start_date.date() == current_date.date():
                    trigger_ETATPARJOUR_SETUP = TriggerDagRunOperator(
                            task_id='trigger_ETATPARJOUR_SETUP',
                                    trigger_dag_id='dagD_2',
                                    wait_for_completion = True
                                    ) 
                else:
                    trigger_ETATPARJOUR_SETUP  = EmptyOperator(
             task_id="bypass_ETATPARJOUR_SETUP",
             )
    
                trigger_ETATPARJOUR = TriggerDagRunOperator(
                        task_id='trigger_ETATPARJOUR',
                                trigger_dag_id='dagD_2',
                                wait_for_completion = True
                                )
                trigger_ETATPARJOURDEMO = TriggerDagRunOperator(
                        task_id='trigger_ETATPARJOURDEMO',
                                trigger_dag_id='dagD_2',
                                wait_for_completion = True
                                )
                trigger_ETATPARJOUR_SETUP >> trigger_ETATPARJOUR >> trigger_ETATPARJOURDEMO

            trigger_FACTPREDICTION >> trigger_DEMOSTOCKVIZ
            trigger_RECORDVIZ >> [trigger_DEMORECORDVIZ, trigger_STOCKVIZ] >> EJDEMOSTOCK

        with TaskGroup('VIZ') as VIZ:
            if start_date.date() == current_date.date():

                trigger_VIZOPERATEURSVELOCITE_SETUP = TriggerDagRunOperator(
                                task_id='trigger_VIZOPERATEURSVELOCITE_SETUP',
                                trigger_dag_id='dagD_2',
                                wait_for_completion = True
                                )
            else:
                trigger_VIZOPERATEURSVELOCITE_SETUP = EmptyOperator(task_id="bypass_VIZOPERATEURSVELOCITE_SETUP")
                
            trigger_VIZOPERATEURSVELOCITE = TriggerDagRunOperator(
                            task_id='trigger_VIZOPERATEURSVELOCITE',
                            trigger_dag_id='dagD_2',
                            wait_for_completion = True
                            )
            
            trigger_DEMO_VIZOPERATEURSVELOCITEWEEK = TriggerDagRunOperator(
                            task_id='DEMO_VIZOPERATEURSVELOCITEWEEK',
                            trigger_dag_id='dagE_2',
                            wait_for_completion = True
                            )
            trigger_DEMO_VIZOPERATEURSVELOCITEDEMO = TriggerDagRunOperator(
                            task_id='DEMO_VIZOPERATEURSVELOCITEDEMO',
                            trigger_dag_id='dagD_2',
                            wait_for_completion = True
                            )
            
            trigger_VIZOPERATEURSVELOCITE_SETUP >>       trigger_VIZOPERATEURSVELOCITE >> [trigger_DEMO_VIZOPERATEURSVELOCITEDEMO,trigger_DEMO_VIZOPERATEURSVELOCITEWEEK ]

    
    @task
    def end ():
        print('Done')

    end = end()

    start >> trigger_STATIC >> [trigger_REDIS, DIMS] >> FACTS >> VIZ_DEMO >> end

master()