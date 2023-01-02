from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import json
import logging

log = logging.getLogger(__name__)
# -----------------------------------------------------------------------------
#   Purpose: Initialize the stored procedure
#   Exception: NONE
#   Returns: {'is_success': True, 'task_instance_id': result["task_instance_id"]}
# -----------------------------------------------------------------------------


def initialize_task_instance(params,**kwargs):
    cursor = None
    log.info("-------params---------")
    log.info(params)
    log.info("-------kwargs---------")
    log.info(kwargs)

    run_history_id = str(kwargs['run_id'])
    dag_id = str(kwargs['dag'])[6:-1]

    if dag_id is None or dag_id == 'null':
        raise Exception("dag_id unavailable")
    try:
        cursor = connect_to_snowflake().cursor()
        # EDIT THE SQLs/PARAMS TO CALL THE STORED PROCEDURES HERE
        sql_command = "CALL sp_intialize_task(%s, %s, %s)"
        sql_params = [run_history_id, str(params["TASK_ID"]), dag_id]
        log.info("-------sql_params---------")
        log.info(sql_params)
        result = execute_commands(cursor, sql_command, sql_params)[0][0]

        result = json.loads(result)
        if result["is_success"]:
            if "task_instance_id" in result:
                log.info(result["task_instance_id"])
                return {'is_success': True, 'task_instance_id': result["task_instance_id"]}
        else:
            log.error(result["error"])
            raise Exception
    except Exception as ex:
        log.error(f"Exception  in initialize_task_instance: {ex}")
        log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
        raise ex
    finally:
        if cursor:
            cursor.close()
