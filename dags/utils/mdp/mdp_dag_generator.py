from snowflake.connector import DictCursor
import sys

#sys.path.append("/Users/deepikan/airflow/mdp_utils")
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands

WORKFLOW_NAME = "DQ_DEMO"
OPS_DB = "DEV_OPS_V3"
OPS_SCHEMA = "CNTRL"

mdp_dag_template = """import sys
import itertools
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG
import datetime


from task_executor import execute_task


default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}}

dag = DAG(
    'mdp_{workflow_name}',
    default_args=default_args,
    description='A simple demo DAG',
    schedule_interval='@once',
    start_date=datetime.datetime(2021, 3, 24),
    is_paused_upon_creation=False
)

start = DummyOperator(
    task_id="Start",
    dag=dag
)

stop = DummyOperator(
    task_id="Stop",
    dag=dag,
)
""".format(workflow_name = WORKFLOW_NAME)

dag_generation_template = """task_groups = []
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
                    f'execute_task_{task_data["TASK_ID"]}', dag, task_section or task_group, task_data)
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
        task_group >> stop
"""


def get_task_groups(cursor):
    sql_command = f"""
    SELECT 
        TASK_GROUP_ID,
        TASK_GROUP_NAME,
        STEP_NUMBER,
        IS_ENABLED,
        CONCURRENCY
    FROM 
	    {OPS_DB}.{OPS_SCHEMA}.TASK_GROUP 
    WHERE 
	    WORKFLOW_ID = (SELECT WORKFLOW_ID FROM {OPS_DB}.{OPS_SCHEMA}.WORKFLOW WHERE WORKFLOW_NAME = '{WORKFLOW_NAME}')
    ORDER BY TASK_GROUP_ID, STEP_NUMBER
    """
    task_groups = execute_commands(cursor, sql_command, [])
    return task_groups


def get_tasks(cursor, task_group_id):
    sql_command = f"SELECT * FROM {OPS_DB}.{OPS_SCHEMA}.VW_TASK_METADATA WHERE TASK_GROUP_ID = '{task_group_id}' ORDER BY STEP_NUMBER"
    tasks = execute_commands(cursor, sql_command, [])
    return tasks


def main():
    cursor = None
    try:
        cursor = connect_to_snowflake().cursor(DictCursor)
        task_groups = get_task_groups(cursor)
        for task_group in task_groups:
            task_group["TASKS"] = get_tasks(
                cursor, task_group["TASK_GROUP_ID"])
        metadata_template = f"""task_group_metadata = {str(task_groups)}"""
        print (metadata_template)
        with open(f"dags/mdp_{WORKFLOW_NAME}.py", "w+") as dag_file:
            dag_file.write(mdp_dag_template + "\n\n" + metadata_template + "\n\n" +
                           dag_generation_template)
    except Exception as ex:
        raise ex
    finally:
        if cursor:
            cursor.close()


if __name__ == "__main__":
    main()
