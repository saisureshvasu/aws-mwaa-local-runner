
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import itertools
import logging
from utils.mdp.task_executor import execute_task
from utils.mdp.dag_util import perform_denodo_cache_refresh
log = logging.getLogger(__name__)

def generate_dag(dag, task_group_metadata, skip_denodo_cache_refresh=False):
    log.info(f"constructing workflow from metadata: {task_group_metadata}")
    log.info(f"constructing workflow with skip_denodo_cache_refresh: {skip_denodo_cache_refresh}")
    try:
        start = DummyOperator(
            task_id="Start",
            dag=dag
        )

        stop = DummyOperator(
            task_id="Stop",
            dag=dag,
        )

        
        task_groups = []
        
        task_group_metadata = sorted(task_group_metadata, key = lambda i: i['STEP_NUMBER'])
        task_group_iterator = itertools.groupby(
            filter(lambda task_group: (task_group["IS_ENABLED"] == 1), task_group_metadata), lambda task_group: task_group["STEP_NUMBER"])
        
        task_group_section_counter = 1
        
        for k, g in task_group_iterator:
            
            task_group_list = list(g)
            task_group_section = None
            if len(task_group_list) > 1:
                task_group_section = TaskGroup(
                    f"tg_section_{task_group_section_counter}", dag=dag)
                task_group_section_counter = task_group_section_counter + 1
                task_groups.append(task_group_section)

            for task_group_data in task_group_list:
                task_group = TaskGroup(
                    task_group_data["TASK_GROUP_NAME"], dag=dag, parent_group=task_group_section)
                task_metadata = task_group_data["TASKS"]
                task_metadata = sorted(task_metadata, key = lambda i: i['STEP_NUMBER'])
                tasks = []
                task_iterator = itertools.groupby(
                    filter(lambda task: task["IS_ENABLED"] == 1, task_metadata), lambda task: task["STEP_NUMBER"])
                task_section_counter = 1
                for key, group in task_iterator:
                    task_list = list(group)
                    task_section = None
                    if len(task_list) > 1:
                        task_section = TaskGroup(
                            f"t_section_{task_section_counter}", dag=dag, parent_group=task_group)
                        task_section_counter = task_section_counter + 1
                        tasks.append(task_section)
                    for task_data in task_list:
                        task = execute_task(
                            f'execute_task_{task_data["TASK_NAME"]}', dag, task_section or task_group, task_data)
                        if not task_section:
                            tasks.append(task)
                for idx, task in enumerate(tasks):
                    if idx == (len(tasks) - 1):
                        pass
                    else:
                        task >> tasks[idx + 1]
                if not task_group_section:
                    task_groups.append(task_group)
        for idx, task_group in enumerate(task_groups):
            if idx == 0:
                start >> task_group
            else:
                task_groups[idx - 1] >> task_group
            if idx == len(task_groups) - 1:
                if skip_denodo_cache_refresh:
                    task_group >> stop 
                else:
                    denodo_cache_refresh = PythonOperator(
                            task_id='denodo_cache_refresh_task',
                            python_callable=perform_denodo_cache_refresh,
                            dag=dag
                    )
                    task_group >> denodo_cache_refresh >> stop 


        
    except Exception as ex:
        log.error("Airflow DAG Exception: Dynamic DAG construction failed")
        raise ex