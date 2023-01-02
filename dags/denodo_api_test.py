from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from utils.denodo_client_util import test_odbc
import logging

log = logging.getLogger(__name__)



try:
    dag_paused = False
    email_list = ['john.eipe@siriuscom.com']
    default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_success':True,
            'email': email_list
            
        }
    catchup=False
    max_active_runs=1
    dag = DAG(
        dag_id='denodo_api_test', 
        default_args=default_args, 
        description='Dag for testing denodo api', 
        catchup=False,
        is_paused_upon_creation=dag_paused,
        start_date=days_ago(1),
        schedule_interval='@once'
    )
    
    
    start = DummyOperator(
        task_id="Start",
        dag=dag
    )
    stop = DummyOperator(
        task_id="Stop",
        dag=dag,
    )
    pyOp = PythonOperator(
        task_id='denodo_fetch_scheduler_jobs_task',
        python_callable=test_odbc,
        params=None,
        dag=dag
    )

    start >> pyOp >>stop  

except Exception as ex:
    log.error("Airflow DAG Exception: denodo_api_test failed")
    log.error(ex)