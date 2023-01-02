from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import json
import logging
import importlib
from airflow.utils.email import send_email
from utils.constants import get_email_list
from utils.parse_variable import get_variable
from inspect import isfunction
log = logging.getLogger(__name__)

def execute_task_instance(params, task_instance, **kwargs):

    log.info("-------params---------")
    log.info(params)
    log.info("-------kwargs---------")
    log.info(kwargs)
    if params["TASK_TYPE"] in ('sp_transform', 'sp_e2e_transform', 'sp_pipe_transform', 'sp_sh_transform','sp_assertion','sp_purge_data','sp_assertion_gold'):
        if params["TASK_TYPE"] == 'sp_transform' or params["TASK_TYPE"] == 'sp_assertion':
            init_details = task_instance.xcom_pull(
                task_ids=f'{params["airflow_group_id"]}.check_ext_files')
        elif params["TASK_TYPE"] == 'sp_pipe_transform':
            init_details = task_instance.xcom_pull(
                task_ids=f'{params["airflow_group_id"]}.check_pipe_load_status')
        else:
            init_details = task_instance.xcom_pull(
                task_ids=f'{params["airflow_group_id"]}.initialize_task_instance')

        log.info("-------init_details---------")
        log.info(init_details)
        cursor = None
        try:
            params_json = params["TASK_PARAMETERS"]
            if (isinstance(params_json,str)):
                params_json = json.loads(params_json)
        
        
            cursor = connect_to_snowflake().cursor()
            
            
            if "last_refresh_date" in init_details:
                last_refresh_date = init_details["last_refresh_date"]
                params_json["sp_params"]["last_refresh_date"] = last_refresh_date


            sql_command = "CALL {tgt_db}.{tgt_schema}.{sp_name}(%s, parse_json(%s), %s)".format(
                tgt_db=params_json["tgt_database"],
                tgt_schema=params_json["tgt_schema"],
                sp_name=params_json["sp_name"])
            sql_params = [init_details["task_instance_id"], json.dumps(params_json["sp_params"]), 0]
            log.info("------sql command--------")
            log.info(sql_command)
            sql_params = [init_details["task_instance_id"], json.dumps(params_json["sp_params"]), 0]
            log.info("------sql params--------")
            log.info(sql_params)
            result = execute_commands(cursor, sql_command, sql_params)[0][0]
            result = json.loads(result)
            log.info(result)
            if not result["is_success"] and "error" in result:
                raise Exception(f"SQL call statement failed for {params_json['tgt_database']}.{params_json['tgt_schema']}.{params_json['sp_name']}")
            # Sending email for DQ if the dq stored proc has successfully run and if the parameter send_dq_email is true    
            elif not result["is_success"] and params["TASK_TYPE"] in ('sp_assertion','sp_assertion_gold') and "rules_status_check" in result and "send_dq_email" in params_json and params_json["send_dq_email"]=='1':
                    message = result["message"]
                    #getting the email list 
                    ops_environment = get_variable("ops_environment")
                    if ops_environment:
                        ops_environment = ops_environment.lower()
                    if (ops_environment == 'dev_ops'):
                        env_name = "Non-Prod"
                    else:
                        env_name = "Prod"
        
                    email_list = get_email_list(ops_environment)
                    
                    total_records = result["total_records"]
                    
                    dq_email_subject = f'{env_name} DQ status for table {result["table_name"]}'
                    email_msg = f'Dag id: {(kwargs["dag"].__dict__)["_dag_id"]} <br/>'
                    email_msg +=  f'DQ status for {result["table_name"]} <br/><br/>'
                    email_msg += f'Total Records:{total_records} <br/><br/>'

                    # If total_records = 0 get the message and no need of the parsing in line #77
                    if (total_records == 0):
                        email_msg+=message
                    
                    else:
                    # parsing the rules_status_check object
                        dq_rules_result = result["rules_status_check"]
                        
                        for rule_result in dq_rules_result:
                            rule_msg =  f'Rule_name: {rule_result["rule"]}' + '<br/>'
                            rule_msg += f'Applied_column: {rule_result["column_name"]}' + '<br/>'
                            rule_msg += f'Pass: {rule_result["is_success"]}' + '<br/>'
                        
                            #rule_msg += f'Required Pass Threshold: {rule_result["pass_threshold"]}' + '<br/>'
                            #If any key in the json that are additional to the above, they will be printed. This can happen in certain dq stored procs. 
                            for additional_key in rule_result:
                                if additional_key not in ['rule','column_name','is_success','total_records','pass_threshold','pass_pct']:

                                    rule_msg += f'{additional_key.title()}: {rule_result[additional_key]}' + '<br/>'
                            email_msg += rule_msg + '<br/><br/>'
        
                    log.info("Sending email for DQ Check" + email_msg )
                    send_email(to=email_list,subject = dq_email_subject,html_content=email_msg)
                    if not result["is_success"] and "pause_on_dq_failure" in params_json and params_json["pause_on_dq_failure"]!='0':

                        raise Exception("DQ checks failed so not continuing with the next step")

                 


            return {
                'is_success': True, 
                'task_instance_id': init_details["task_instance_id"]
            }
        except Exception as ex:
            log.error(f"Exception  in execute_task_instance: {ex}")
            log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
            return {
                'is_success': False, 
                'task_instance_id': init_details["task_instance_id"],
                'ex':str(ex)
            }
        finally:
            if cursor:
                cursor.close()
    elif params["TASK_TYPE"] in ('api_ingestion', 'archive_s3_files', 'ftp_file_upload', 's3_file_upload', 'py_exec_task'):
        try:
            init_details = task_instance.xcom_pull(
                task_ids=f'{params["airflow_group_id"]}.initialize_task_instance')
            log.info("-------init_details---------")
            log.info(init_details)
            run_history_id = str(kwargs['run_id'])
            log.info(run_history_id)
            params_json = params["TASK_PARAMETERS"]
            log.info("Function to be executed: {}".format(params_json['fn_name']))
            
            module = importlib.import_module("utils.domain.delegate")
            for attribute_name in dir(module):
                attribute = getattr(module, attribute_name)
                if isfunction(attribute) and attribute_name == params_json['fn_name']:
                    attribute(params_json['params'], init_details["task_instance_id"], run_history_id)
                else:
                    log.debug("attribute: {} and attribute name: {}".format(attribute, attribute_name))

            return {'is_success': True, 'task_instance_id': init_details["task_instance_id"]}
        except Exception as ex:
            log.error(f"Exception  in execute_task_instance: {ex}")
            log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
            return {
                'is_success': False, 
                'task_instance_id': init_details["task_instance_id"],
                'ex':str(ex)
            }
