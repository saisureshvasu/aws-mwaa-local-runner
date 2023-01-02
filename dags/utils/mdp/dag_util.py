from requests.models import Response
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import json
import itertools
from airflow.utils.email import send_email
from utils.parse_variable import get_variable
from utils.constants import get_email_list,get_excluded_tbl_names
from utils.denodo_client_util import *
from utils.mdp.denodo_model import DenodoModel
import logging
log = logging.getLogger(__name__)

def check_ext_files(params, task_instance,**kwargs):
    cursor = None
    try:
        log.info("-------params---------")
        log.info(params)
        cursor = connect_to_snowflake().cursor()

        context = kwargs

        init_details = task_instance.xcom_pull(
            task_ids=f'{params["airflow_group_id"]}.initialize_task_instance')
        
        log.info("-------init_details---------")
        log.info(init_details)
        params_json = params["TASK_PARAMETERS"]
        if (isinstance(params_json,str)):
            params_json = json.loads(params_json)
        
        
        log.info("------sql command--------")
       
        sql_params = []
        tbl_name = params_json["sp_params"]["src_tbl"]
        schema_name = params_json["sp_params"]["src_schema"]
        db_name =  params_json["sp_params"]["src_db"]
        #sp_assertion is for DQ
        if (params["TASK_TYPE"] == "sp_assertion"):
            sp_name = 'SP_DQ_PRERUN_CHECK'
        #sp_transform is for data load
        elif (params["TASK_TYPE"] == "sp_transform"):
            
            sp_name = 'SP_GET_EXT_FILE_INFO'
            
        
        sql_command = "CALL {db}.{schema}.{sp_name} (%s , %s, %s, %s, %s, %s)".format(
            sp_name = sp_name,
            db=params_json["tgt_database"],
            schema=params_json["tgt_schema"]
        )
        log.info(sql_command)
        sql_params = [db_name,schema_name,tbl_name, context['run_id'], init_details["task_instance_id"],0]
            
            
        
        log.info("------sql params--------")
        log.info(sql_params)
        
        result = execute_commands(cursor, sql_command, sql_params)[0][0]

        result = json.loads(result)
        log.info(result)
        if result["is_success"]:
            
            log.info(result["is_success"])
            result_details = result["metadata"]
            if "number_files_arrived" in result_details:
                log.info("number_files_arrived",result_details["number_files_arrived"])
                # Get the excluded table names and if its there no need to send the email
                excluded_tbl_names  = get_excluded_tbl_names()

                if tbl_name not in excluded_tbl_names:
                    # Invoking fn. to check if email needs to be sent
                    log.info("email can be sent")
                    email_callback(result_details,db_name,schema_name,tbl_name,result_details['last_ext_table_refresh_time'],result_details["last_refresh_dateformat"])
            #This is return is for the DQ checks where we need the last_refresh_date   
            if "last_refresh_dateformat" in result_details:
                return {'is_success': True, 
                'task_instance_id': init_details["task_instance_id"],
                "last_refresh_date":result_details["last_refresh_dateformat"]
                }
            else:
                return {'is_success': True, 
                'task_instance_id': init_details["task_instance_id"]}


        else:
            log.error(result["error"])
            return {'is_success': False, 'task_instance_id': init_details["task_instance_id"],'ex':result["error"]}
        
    except Exception as ex:
        log.error("Airflow DAG Exception: check ext file failed")
        log.error(ex)
        return {'is_success': False, 'task_instance_id': init_details["task_instance_id"],'ex':str(ex)}
    finally:
        if cursor:
            cursor.close()


def email_callback(result,src_db,src_schema,src_tbl,fileArrivalTime,lastLoadTime):
    try:
        log.info(" entering email_callback")
        env_name = ''
        send_mail = False
        email_list =[]
        content = ''
        ops_environment = get_variable("ops_environment")
        if ops_environment:
            ops_environment = ops_environment.lower()
            email_list = get_email_list(ops_environment)
            if (ops_environment == 'dev_ops'):
                
                env_name = "Non-Prod"
            else:
                
                env_name = "Prod"
        
    

        if (result["number_files_arrived"] == 0):
            content = '<b>No files received for the table : {src_db}.{src_schema}.{src_tbl} </b><br><br> Last File Arrival time in S3: {fileArrivalTime} <br><br> Last External Table Refresh time: {lastLoadTime}'.format(src_db=src_db,src_schema=src_schema,src_tbl=src_tbl,fileArrivalTime=fileArrivalTime,lastLoadTime=lastLoadTime) 
            send_mail = True
        
        elif ("file_count_diff_result" in result):
            log.info(" entering count_diff")
            avg_count = result["avg_file_count"]
            if(result["file_count_diff_result"] == True):
                diff_result = "Number of files received is less than 10% of average number of files for the table : {src_db}.{src_schema}.{src_tbl} for the last 30 days".format(src_db=src_db,src_schema=src_schema,src_tbl=src_tbl)
                send_mail = True
                content = '<br><b>File Load status</b><br>' + diff_result + '<br>Average Number of Files Received for the last 30 days : {avg_count}<br> Number of Files Received Today {file_count}'.format(avg_count = avg_count,file_count=result["number_files_arrived"])

        elif (result["number_files_arrived"] > 0 and result["record_count"] == 0 ):
            content = '<b>Files Count for the table {src_db}.{src_schema}.{src_tbl} : {num_files}. No rows to insert ! </b> <br> Last File Arrival Time in S3: {fileArrivalTime} <br> Last External Table Refresh time: {lastLoadTime}'.format(src_db=src_db,src_schema=src_schema,src_tbl=src_tbl,num_files=result["number_files_arrived"],fileArrivalTime= fileArrivalTime,lastLoadTime=lastLoadTime)
            send_mail = True
        # todo - remove this part after testing
        elif (len(src_tbl) == 0 and result["number_files_arrived"] =='NA' and result["record_count"] == 'NA' ):
           log.info("Re-engineering table")
           return

        log.info("email content: "+content)
        
        if (send_mail):
            send_email(
                to=email_list,
                subject = env_name + ' Airflow Alert - External File Status for the table {src_db}.{src_schema}.{src_tbl}'.format(src_db=src_db,src_schema=src_schema,src_tbl=src_tbl),
                html_content=content,
            )
        
        log.info(" exiting email_callback")
    except Exception as ex:
        log.error("Airflow DAG Exception: email callback failed")
        log.error(ex)

# checking and invoking the cache after every task is still effective
# jobs will run only once. eg: 
#
def perform_denodo_cache_refresh(**kwargs):
    
    cursor = None


    current_dag_id = str(kwargs['dag'])[6:-1]
    run_history_id = str(kwargs['run_id'])

    
    try:
              
        sql_command = """select JOB_CONFIG_ID, JOB_ID, PROJECT_ID, JOB_NAME, VIEW_CACHED from DENODO_JOB_CONFIG 
        where IS_ENABLED=TRUE and array_contains('{current_dag_id}'::variant, dag_list)""".format(current_dag_id=current_dag_id)
            
        log.info(sql_command)
        sql_params = []

        cursor = connect_to_snowflake().cursor()
        result = execute_commands(cursor, sql_command, sql_params)

        views_to_be_refreshed = dict()

        if len(result)==0:
            log.warn("Airflow DAG Exception: Denodo cache refresh criteria not met for DAG id: {}".format(current_dag_id))
            return {'is_success': False}

        for row in result:
            views_to_be_refreshed[row[4]] = DenodoModel(row[0], row[1], row[2], row[3], row[4])

        
        # below approach is for dynamic job information from dendo
        #scheduler_jobs = denodo_fetch_scheduler_jobs()

        # fetch the job id and project id for the views identified
        # for scheduler_job in scheduler_jobs:
        #     try:
        #         if  scheduler_job['type'] == "VDPCache" and scheduler_job['id'] in job_ids:
                    
        #             print("0. View information available for jobId: {} and projectId: {}".format(scheduler_job['id'], scheduler_job['projectId']))
        #             print("Scheduler job: {}".format(scheduler_job['extractionSection']) )
        #             print("View name: {}".format(scheduler_job['extractionSection']['loadprocesses'][0]['view']))
        #             view = scheduler_job['extractionSection']['loadprocesses'][0]['view']
        #             views_to_be_refreshed[view] = DenodoModel(scheduler_job['id'], scheduler_job['projectId'], view)
        #             # invoke denodo_cache_refresh
        #         else:
        #             print("1. View information NOT available for jobId: {} and projectId: {}".format(scheduler_job['id'], scheduler_job['projectId']))
                    
        #     except KeyError:
        #         print("2. View information NOT available for jobId: {} and projectId: {}".format(scheduler_job['id'], scheduler_job['projectId']))

        
        log.info(views_to_be_refreshed)

        # fetch denodo views and table dependencies
        elements = denodo_fetch_object_dependency()
        for element in elements:
            if element['view_name'] in views_to_be_refreshed.keys():
                log.info("View name information available: {}".format(element))
                denodo_model = views_to_be_refreshed[element['view_name'] ]
                denodo_model.source_tables.add(element["source_catalog_name"]+"."+element["source_schema_name"]+"."+element["source_table_name"])
                log.info("Denodo model updated: {}".format(denodo_model))

        # the run should have happened within last 24 hours with data load and the latest run should be success
        sql_command = """select TGT_DB || '.' || TGT_SCHEMA || '.' || TGT_TBL as TBL   
            from (select t.* , rank() over(partition by tgt_tbl order by END_TIME desc) as r
            from VW_TASK_RUN_LOG t
            where TASK_TYPE = 'sp_transform'
            )
            where r=1
            and PERFORM_DATA_LOAD ='true' 
            and END_TIME > dateadd(hour, -24, current_timestamp)
            and (to_number(nvl(INSERT_ROW_COUNT,0))>0 or  to_number(nvl(UPDATE_ROW_COUNT,0))>0)"""
            
        log.info(sql_command)
        sql_params = []

        cursor = connect_to_snowflake().cursor()
        result = execute_commands(cursor, sql_command, sql_params)
        tables_ready = []
        for row in result:
            tables_ready.append(row[0])

        log.info("Tables ready: {}".format(tables_ready))

        # check if current table is part of any source_tables
        for view_name, denodo_model in views_to_be_refreshed.items():
            log.info("Analyzing object: {}".format(denodo_model))
            is_view_refresh_ready = True
 
            # check if  tables in the set is already complete
            # if complete then trigger refresh
                
            for source_table in denodo_model.source_tables:
                log.info("Analyzing table: {}".format(source_table))
                if source_table.startswith('EDW_SS'):
                    continue
                if source_table not in tables_ready:
                    log.info("Table {} has not been loaded in the last 24 hours".format(source_table))
                    is_view_refresh_ready = False
                    break

            if len(denodo_model.source_tables) != 0 and is_view_refresh_ready:
                denodo_cache_refresh(denodo_model.job_id, denodo_model.project_id)
                cursor = connect_to_snowflake().cursor()

                sql_command = "insert into denodo_job_run_history(job_config_id, run_history_id, dag_id, trigger_dttm, trigger_status) values (%s, %s, %s, current_timestamp, 'success')"
                sql_params = [int(denodo_model.job_config_id), run_history_id, current_dag_id]
                execute_commands(cursor, sql_command, sql_params)


        return {'is_success': True}

    except Exception as ex:        
        log.error("Airflow DAG Exception: Denodo cache refresh failed for DAG: {}".format(current_dag_id))
        raise ex
    finally:
        if cursor:
            cursor.close()

def add_file_ingestion_log(task_instance_id, tgt_tbl, file_names, run_history_id):
    cursor = None
    try:
        cursor = connect_to_snowflake().cursor()
        sql_command = """insert into file_ingestion_history_log(task_instance_id, tgt_tbl, file_names, ingested_ts, run_history_id) 
        select $1, $2, array_construct($3), $4, $5
        from values (%s, %s, %s, current_timestamp, %s)"""
        sql_params = [task_instance_id, tgt_tbl, file_names, run_history_id]
        execute_commands(cursor, sql_command, sql_params)
    except Exception as ex:
        raise ex
    finally:
        if cursor:
            cursor.close()
    """
        This function is to check if there are any files 
        loaded into the pipe through snowpipe. 
        SP => SP_ADD_RAW_TABLE_LOAD_LOG
    """
def check_pipe_load_status(params, task_instance,**kwargs):
        try:
            log.info("-------params---------")
            log.info(params)
            cursor = connect_to_snowflake().cursor()
            init_details = task_instance.xcom_pull(
                task_ids=f'{params["airflow_group_id"]}.initialize_task_instance')
            
            log.info("-------init_details---------")
            log.info(init_details)

            params_json = params["TASK_PARAMETERS"]
            if (isinstance(params_json,str)):
                params_json = json.loads(params_json)

            sp_params_json = {}
            sp_params_json["src_tbl"] = params_json["sp_params"]["src_tbl"]
            sp_params_json["src_schema"] = params_json["sp_params"]["src_schema"]
            sp_params_json["src_db"] = params_json["sp_params"]["src_db"]
            sp_params_json["tgt_tbl"] = params_json["sp_params"]["tgt_tbl"]
            sp_params_json["tgt_schema"] = params_json["sp_params"]["tgt_schema"]
            sp_params_json["tgt_db"] = params_json["sp_params"]["tgt_db"]
            sp_params_json["ingestion_dttm_col"] = params_json["sp_params"]["ingestion_dttm_col"]
            run_history_id = str(kwargs['run_id'])
            sp_name = "SP_ADD_RAW_TABLE_LOAD_LOG"
            
            
            sql_command = "CALL {tgt_db}.{tgt_schema}.{sp_name}(%s, parse_json(%s), %s,%s)".format(
                tgt_db=params_json["tgt_database"]  #ops db - devops/prodops
                ,
                tgt_schema=params_json["tgt_schema"]    #ops schema cntrl
                ,
                sp_name=sp_name)
            sql_params = [init_details["task_instance_id"], json.dumps(sp_params_json), 0,run_history_id]
            log.info("------sql params--------")
            log.info(sql_params)
            
            result = execute_commands(cursor, sql_command, sql_params)[0][0]

            result = json.loads(result)
            log.info(result)
            # Here the logic should be similar to check_ext_File to send the email 
            if result["is_success"]:
            
                log.info(result["is_success"])
                result_details = result["metadata"]
                if "number_files_arrived" in result_details:
                    log.info("number_files_arrived",result_details["number_files_arrived"])
                    email_callback(result_details,
                    params_json["sp_params"]["src_db"],
                    params_json["sp_params"]["src_schema"],
                    params_json["sp_params"]["src_tbl"],
                    result_details["pipe_received_ts"],
                    result_details["last_load_ts"]
                    )
                return {'is_success': True, 'task_instance_id': init_details["task_instance_id"]}
            else:
                log.error(result["error"])
                return {'is_success': False, 'task_instance_id': init_details["task_instance_id"],'ex':result["error"]}
            
        except Exception as ex:
            log.error("Airflow DAG Exception: check ext file failed")
            log.error(ex)
            return {'is_success': False, 'task_instance_id': init_details["task_instance_id"],'ex':str(ex)}
        finally:
            if cursor:
                cursor.close()
            


        
