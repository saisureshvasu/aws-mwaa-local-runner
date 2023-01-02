from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import timedelta
from airflow import AirflowException
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import logging

log = logging.getLogger(__name__)
class CopyCheckSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, 
        timeout=1*60*60, 
        mode = "reschedule", 
        poke_interval = 5,
        *args,
        **kwargs):
        super(CopyCheckSensor, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.mode = mode
        self.timeout = timeout
        log.info(args)
        log.info(kwargs)


    def poke(self, context):
        """poke is a overridden method
            poke is expected to return a boolean that indicates if a success criteria is met or not. 
            If it is met, the sensor will set the tasks’ state to Success and will end the execution. 
            If the return of the poke is False, the sensor will continue polling based on one of two modes defined later on.
        Args:
            context (Dict): populated automatically (similar to **kargs)

        Raises:
            AirflowException: this is raised only if status is None or empty

        Returns:
            boolean: indicating if the sensor should exit if True else poll again after the poll_interval
        """
        print("________________CopyCheckSensor________________")
        print(f'poke_interval is set to {self.poke_interval} sec')
        print(f'mode is set to {self.mode}')
        print("__________________________________________________")
        print("_____________________context______________________")
        print(context)
        run_history_id = context['run_id']
        src_tbl = context["params"]["TASK_PARAMETERS"]["sp_params"]["src_tbl"]
        src_db = context["params"]["TASK_PARAMETERS"]["sp_params"]["src_db"]
        src_schema = context["params"]["TASK_PARAMETERS"]["sp_params"]["src_schema"]
        print(run_history_id)
        print(src_tbl)
        status = self.fetch_copy_status(src_db, src_schema, src_tbl, run_history_id)
        print('status of application is ->', status)
        if status:
            print(f"Job returned from waiting state succesfully!")
            return True
        if status is None or status == '':
            raise AirflowException(f"Job did note run")
        return False

    def fetch_copy_status(self, src_db, src_schema, src_tbl, run_history_id):
        """utility method that checks the copy history status
        Args:
            src_tbl (string): table populated by the copy or pipe command

        Raises:
            Exception: generic exception 

        Returns:
            boolean: indicating if ingestion is complete
        """
        log.info("fetch_copy_status")
        cursor = None
        try:
            # SQL
            # check the ingested FILE_NAMES and INGESTED_TS from FILE_INGESTION_HISTORY_LOG using the src_tbl and run_history_id
            # and check with the copy_history response using the src_tbl and min(INGESTED_TS)  
            sql_command = """
                with cte1 as (
                    select fn.value::varchar as FILE_NAME, INGESTED_TS from FILE_INGESTION_HISTORY_LOG src,
                    lateral flatten(input => src.file_names) fn
                    where TGT_TBL = '{tbl_name}' AND RUN_HISTORY_ID = '{run_history_id}'
                    order by INGESTED_TS desc
                )
                , cte2 as (
                    select min(INGESTED_TS) as MIN_INGESTED_TS from cte1
                )
                select cte1.FILE_NAME, copyhist.FILE_NAME from table(INFORMATION_SCHEMA.COPY_HISTORY (table_name=>'{src_db}.{src_schema}.{src_tbl}', start_time=> dateadd(hours, -24, current_timestamp()))) copyhist
                right outer join cte1 on copyhist.FILE_NAME = cte1.FILE_NAME
                where last_load_time>=(select dateadd(hour, -1, MIN_INGESTED_TS) from cte2 ) """.format(
                run_history_id=run_history_id,
                tbl_name=src_tbl,
                src_db=src_db, 
                src_schema=src_schema, 
                src_tbl=src_tbl )
           
            log.info(sql_command)
            sql_params = []

            cursor = connect_to_snowflake().cursor()
            result = execute_commands(cursor, sql_command, sql_params)
            if len(result)==0:
                log.info(f'None of the files recieved')
                return False
            log.info(result)
            log.info(len(result))
            files_processed_counter = 0
            for row in result:
                print(f'Comparing ingestion time {row[0]} to last load time {row[1]}')
                if row[0]!=row[1]:
                    log.info(f'Files not recieved corresponding to {row[0]}')
                    return False
                else:
                    log.info(f'Files recieved corresponding to {row[0]}')
                    files_processed_counter=files_processed_counter+1
            if files_processed_counter == len(result):
                log.info("All files loaded into target table")
                return True
        except Exception as ex:
            log.error("Airflow DAG Exception: copy_check_sensor failed")
            raise ex
        finally:
            if cursor:
                cursor.close()