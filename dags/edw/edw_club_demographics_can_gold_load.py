from airflow import DAG
import logging
import os
import importlib
from utils.parse_variable import get_variable
from utils.mdp.construct_dag import generate_dag
from utils.constants import get_email_list
from utils.email_util import send_custom_failure_email
from datetime import datetime

log = logging.getLogger(__name__)

"""
    Load CLUB_DEMOGRAPHICS_CAN_STG raw files into CLUB_DEMOGRAPHICS_CAN_STG gold table
"""
try:
    
    ops_environment = get_variable("ops_environment")
    
    DAG_FILE_NAME = os.path.basename(__file__).replace(".py", "")

    email_list =[]    
    if ops_environment:
        ops_environment = ops_environment.lower()
        email_list = get_email_list(ops_environment)
        metadata = importlib.import_module("edw.metadata." + DAG_FILE_NAME + "_metadata")
        DEFAULT_ARGS = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email': email_list,
            'email_on_failure': False,
            'email_on_success':True,
            'on_failure_callback': send_custom_failure_email
            
        }
 
    dag = DAG(
        dag_id = DAG_FILE_NAME,
        doc_md = __doc__,
        default_args = DEFAULT_ARGS,
        description='Dag for loading CLUB_DEMOGRAPHICS_CAN_STG raw files into CLUB_DEMOGRAPHICS_CAN_STG gold table',
        tags = ['edw', 'club_demographics_can_stg'],
        schedule_interval='30 02 * * *',
        start_date = datetime(2022,12,20),
        is_paused_upon_creation=False,
        catchup=False,
        max_active_runs = 1
    )

    generate_dag(dag, metadata.task_group_metadata[ops_environment])
    
except Exception as ex:
    log.error("Airflow DAG Exception: {} failed".format(DAG_FILE_NAME))
    log.error(ex)