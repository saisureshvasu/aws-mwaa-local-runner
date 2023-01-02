from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import json
import logging

log = logging.getLogger(__name__)
def update_task_instance(params, task_instance, **kwargs):
    cursor = None
    try:
        cursor = connect_to_snowflake().cursor()
        # EDIT THE SQLs/PARAMS TO CALL THE STORED PROCEDURES HERE
        sql_command = "CALL sp_update_task_instance(%s, %s)"
        sql_params = None
        is_error = False
        exec_details = task_instance.xcom_pull(
            task_ids=f'{params["airflow_group_id"]}.execute_task_instance')
        if exec_details["is_success"]:
            sql_params = [exec_details["task_instance_id"], 'completed']
        else:
            is_error = True
            sql_params = [exec_details["task_instance_id"], 'failed']
        result = execute_commands(cursor, sql_command, sql_params)[0][0]
        result = json.loads(result)
        if (is_error):
            raise Exception

        if result["is_success"]:
            return {'is_success': True, 'task_instance_id': exec_details["task_instance_id"]}
        else:
            log.error(result["error"])
            raise Exception
    except Exception as ex:
        log.error(f"Exception  in update_task_instance: {ex}")
        log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
        raise ex
    finally:
        if cursor:
            cursor.close()
