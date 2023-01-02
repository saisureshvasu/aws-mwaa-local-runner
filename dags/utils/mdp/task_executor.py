from email import utils
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from utils.mdp.task_instance_initializer import initialize_task_instance
from utils.mdp.task_instance_executor import execute_task_instance
from utils.mdp.task_instance_manipulator import update_task_instance
import utils.mdp.dag_util as dag_util
from utils.sensors.copy_check_sensor import CopyCheckSensor
import logging
log = logging.getLogger(__name__)



def execute_task(task_group_name, dag, parent_group, task_params):
    """Creates mini task pipelines for each task within a group

    All tasks are wrapped within a python operator and preceeded by an initalization process and succeded by an updation process.
    A task could be of the following types:
        - api_ingestion         : if you are using a python function to ingest from API
        - sp_e2e_transform      : if you are using stored procedure transformation after a pipe trigger (the prior step i.e., ingestion is controlled as part of the workflow)
        - sp_pipe_transform     : if you are using stored procedure transformation after a pipe trigger (the prior step i.e., ingestion is not controlled as part of the workflow)
        - sp_sh_transform       : if you are loading from a snowflake share
        - sp_ss_transform       : if you are loading from a snowflake stream
        - sp_transform          : if it is a, external table load
        - sp_assertion          : if it is a DQ assertion on the table
        - archive_s3_files      : if s3 files needs to be archived
        - ftp_file_upload       : if it is a file upload to ftp
        - s3_file_upload        : if it is a file upload to s3
        - sp_purge_data         : if you are deleting data using stored procedures on snowflake
        - sp_assertion_gold     : if it is a DQ assertion on the gold table
        - py_exec_task          : if you need to run a python function for performing any task (generic)
         

    Args:
        task_group_name (String): the task group
        dag (DAG): airflow DAG object
        parent_group (TaskGroup): airflow TaskGroup object
        task_params (dict): dict representing task parameters

    Returns:
        _type_: TaskGroup
    """
    with TaskGroup(task_group_name, dag=dag, parent_group=parent_group) as task_group:
        task_params["airflow_group_id"] = task_group.group_id
        
        initalize_task = PythonOperator(
            task_id='initialize_task_instance',
            python_callable=initialize_task_instance,
            params=task_params,
            dag=dag
        )

        execute_task = PythonOperator(
            task_id='execute_task_instance',
            python_callable=execute_task_instance,
            params=task_params,
            dag=dag
        )

        update_task = PythonOperator(
            task_id='update_task_instance',
            python_callable=update_task_instance,
            params=task_params,
            dag=dag
        )
        if task_params["TASK_TYPE"] == 'sp_e2e_transform' or task_params["TASK_TYPE"] == 'sp_pipe_transform':
            check_pipe_load_status = PythonOperator(
                task_id='check_pipe_load_status',
                python_callable= dag_util.check_pipe_load_status,
                params=task_params,
                dag=dag
            )
        
        if task_params["TASK_TYPE"] == 'sp_assertion' or task_params["TASK_TYPE"] == 'sp_transform':
            check_ext_files = PythonOperator(
                task_id='check_ext_files',
                python_callable= dag_util.check_ext_files,
                params=task_params,
                dag=dag
            )
        
        ## building task group pipeline    
        if task_params["TASK_TYPE"] == 'sp_assertion' or task_params["TASK_TYPE"] == 'sp_transform':
            initalize_task >> check_ext_files >> execute_task >> update_task
        elif task_params["TASK_TYPE"] == 'sp_e2e_transform':
            copy_check_sensor = CopyCheckSensor(
                task_id='copy_check_sensor',
                params=task_params,
                dag=dag
                )
            initalize_task >> copy_check_sensor >> check_pipe_load_status >> execute_task >> update_task
        elif task_params["TASK_TYPE"] == 'sp_pipe_transform':
            initalize_task >> check_pipe_load_status >> execute_task >> update_task
       
        else:
            # default
            initalize_task >> execute_task >> update_task

        return task_group

