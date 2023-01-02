import json
import logging
import io
import csv
from datetime import datetime,timedelta
import pandas as pd
from utils.s3_client_util import S3ClientUtil
from utils.domain.leadgen_util import LeadGenUtil
from utils.domain.fb_util import FBUtil
from utils.domain.cms_util import CMSUtil
from utils.domain.sfmc_util import SFMCUtil
from utils.domain.services_util import ServicesUtil
from utils.parse_variable import get_variable
from utils.mdp.dag_util import add_file_ingestion_log
from utils.constants import get_email_list
from utils.s3_client_util import S3ClientUtil
from utils.cms_client_util import CMSClientUtil
from utils.parse_variable import get_variable
from utils.fb_client_util import FBClientUtil
from utils.bq_client_util import BQClientUtil
from utils.mdp.dag_util import add_file_ingestion_log
from google.cloud import bigquery
from google.oauth2 import service_account
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
from airflow.utils.email import send_email
from utils.constants import get_agency_email_list
from utils.constants import SPENDS_AGENCIES
from utils.domain.sftp_source import SFTPClubDemographics

log = logging.getLogger(__name__)


  
def get_cms_api_data(params, task_instance_id, run_history_id):
    """Call CMS API and download file to S3
            1. fetches API data
            2. loads JSON file into s3
    Args:
        params (dict): dictionay object with parameter details
    """
    log.info(f'Executing get_cms_api_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
    raw_bucket = get_variable('raw_bucket')

    raw_s3_path = params['raw_s3_path']
    headers = params['headers']
    request_type = params['request_type']
    space_id = params['space_id']
    output_file_name = params['output_file_name']
    base_url = params['base_url']
    tgt_tbl = params['tgt_tbl']
    url = f'{base_url}/spaces/{space_id}/entries'

    cmsClientUtil = CMSClientUtil().instance()

    api_params = {
        'access_token': cmsClientUtil.access_token,
        'content_type': params['content_type'],
        'skip': 0,
        'limit': 1000,
        'include': 10
    }

    current_record_count = api_params['limit']
    try:
        while True:
            data = None
            log.info(f"Calling CMS API for content type {params['content_type']}")
            response = cmsClientUtil.get_data(request_type,
                                     url,
                                     headers=headers,
                                     api_params=api_params)

            if response.status_code == 200:
                log.info(f"Response code = 200")
                data = response.text
                json_data = json.loads(data)
                total = json_data['total']
            else:
                log.info(f"Received Response :{response.status_code}")
                break

            #The content_type for cms_codes is stringTranslationList
            if params['content_type'] == 'sport':
                json_str = CMSUtil.flatten_sport_json(data)
            elif params['content_type'] == 'exercise':
                json_str = CMSUtil.flatten_exercise_json(data) 
            elif params['content_type'] == 'longFormWorkout':
                category_api_params = {
                    'access_token': cmsClientUtil.access_token,
                    'content_type': 'category',
                    'skip': 0,
                    'limit': 1000,
                    'include': 10
                }
                category_data = None
                category_response = cmsClientUtil.get_data(request_type,
                                     url,
                                     headers=headers,
                                     api_params=category_api_params)
                if category_response.status_code == 200:
                    log.info(f"Response code = 200")
                    category_data = category_response.text
                    
                else:
                    log.info(f"CMS Category API call failed \n Received Response :{category_response.status_code}")
                    break
                json_str = CMSUtil.flatten_routine_workoutLayout_json(data,category_data) 
            elif params['content_type'] == 'standardWorkout':
                json_str = CMSUtil.flatten_routine_standardWorkout_json(data)
            elif params['content_type'] == 'perks':
                json_str = CMSUtil.flatten_perks_json(data)
            elif params['content_type'] == 'livestream':
                json_str = CMSUtil.flatten_livestream_json(data)
            elif params['content_type'] == 'promotions':
                json_str = CMSUtil.flatten_promotion_json(data)            
            elif params['content_type'] == 'stringTranslationList':
                json_str = CMSUtil.flatten_cms_codes_json(data)
            bytes_data = bytes(json_str, 'utf-8')
            if bytes_data:
                s3ClientUtil = S3ClientUtil.instance()
                file_name = f'{datetime.utcnow().strftime("%Y%m%d")}/{output_file_name}_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.json'
                log.info(f'Writing data to S3 path - {raw_s3_path}/{file_name}')
                s3ClientUtil.write_data(raw_bucket, f'{raw_s3_path}/{file_name}', bytes_data)
            else:
                log.error(f'Failed to create file {file_name}')
            
            # If total records is greater than limit call API with skip = limit
            # Loop until current_record_count <= 0
            current_record_count = total - api_params['limit'] - api_params['skip']
            api_params['skip'] = api_params['skip'] + api_params['limit']
            if current_record_count <= 0:
                break
        add_file_ingestion_log(task_instance_id, tgt_tbl, file_name, run_history_id)    
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in get_cms_api_data delegate function")


def archive_files(params, task_instance_id=None, run_history_id=None):
    """Archive files from input S3 folder to archival folder
    Args:
        params (dict): dictionay object with parameter details
    """  
    try:
        log.info(f'Executing archive_files for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        if 's3_bucket' in params.keys():
            s3_bucket = params['s3_bucket'] 
        else:
            s3_bucket = None
        if s3_bucket is None:
            raw_bucket = get_variable('raw_bucket') #defaults to raw bucket
        else:
            raw_bucket = get_variable(s3_bucket)
         
        raw_s3_path = params['raw_s3_path']
        archive_path = params['archive_s3_path']
        days_to_archive_for = params['days_to_archive_for']


        s3ClientUtil = S3ClientUtil.instance()
        log.info(f'List files under {raw_bucket}/{raw_s3_path}')
        list_of_objects = s3ClientUtil.get_all_objects(raw_bucket,raw_s3_path)
        log.info(list_of_objects)
        archive_date = (datetime.now() - timedelta(days=days_to_archive_for)).date()
        log.info(f'Archiving files after {archive_date}')

        spends_tables = ['NATIONAL_SPENDS_PLACED', 'LOCAL_SPENDS_PLACED', 'LOCAL_SPENDS_PLANNED']
        for key, key_date in list_of_objects:
            if params['tgt_tbl'] in spends_tables:

                ingestion_date = datetime.strptime(''.join(key.split('/')[-4:-1]), '%Y%m%d').date()
            else: 
                ingestion_date = datetime.strptime(key.split('/')[-2], '%Y%m%d').date()
            log.info(f'Key = {key}, ingestion date = {ingestion_date}')
            if ingestion_date <= archive_date:
                if params['tgt_tbl'] in spends_tables:
                    s3ClientUtil.move_files(raw_bucket, key, raw_bucket,f"{archive_path}/{'/'.join(key.split('/')[-4:])}")
                    log.info(f'Moved {key} to archive location {archive_path}/{key}')
                else:
                    s3ClientUtil.move_files(raw_bucket, key, raw_bucket,f"{archive_path}/{'/'.join(key.split('/')[-2:])}")
                    log.info(f'Moved {key} to archive location {archive_path}/{key}')


    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in archive_files delegate function")

def pfservices_member_type_dim_file_upload(params, task_instance_id, run_history_id):
    """Reads the given src table and uploads it to s3 location for PF Services
        in csv format
    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    try:
        log.info(f'Executing pfservices_member_type_dim_file_upload for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        log.info(params)
        s3_path = params['s3_path']
        stage_name = params['stage_name']
        src_db = params['src_db']
        src_schema = params['src_schema']
        src_tbl = params['src_tbl']
        file_name = 'member_type_dim.csv'
        if all(v is None or '' for v in [ s3_path, stage_name, src_tbl, src_db, src_schema]):
            log.error(f"Required params are not available: {params}")

        ServicesUtil.upload_sf_member_type_dim_to_s3(s3_path, file_name, stage_name, src_tbl, src_db, src_schema)
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in pfservices_member_type_dim_file_upload delegate function")

def pfservices_crowd_meter_file_upload(params, task_instance_id, run_history_id):
    """Reads the given src table and uploads it to s3 location for PF Services
        in csv format
    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    try:
        log.info(f'Executing pfservices_crowd_meter_file_upload for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        log.info(params)
        s3_path = params['s3_path']
        stage_name = params['stage_name']
        src_db = params['src_db']
        src_schema = params['src_schema']
        src_tbl = params['src_tbl']
        file_name = 'crowd_meter_trend.csv'
        if all(v is None or '' for v in [s3_path, stage_name, src_db, src_schema]):
            log.error(f"Required params are not available: {params}")

        ServicesUtil.upload_sf_crowd_meter_to_s3(s3_path, file_name, stage_name, src_tbl, src_db, src_schema)
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in pfservices_member_type_dim_file_upload delegate function")

def sfmc_sftp_marketing_journey_file_upload(params, task_instance_id, run_history_id):
    """Reads the given src table and uploads it to sftp location for SFMC [Salesforce Marketing Cloud]
        in csv format
    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    try:
        log.info(f'Executing sfmc_sftp_marketing_journey_file_upload for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        log.info(params)
        s3_bucket = params['s3_bucket']
        s3_path = params['s3_path']
        stage_name = params['stage_name']
        drop_location = params['drop_location']
        src_db = params['src_db']
        src_schema = params['src_schema']
        if all(v is None or '' for v in [s3_bucket, s3_path, stage_name, drop_location, src_db, src_schema]):
            log.error(f"Required params are not available: {params}")

        SFMCUtil.upload_hssp_marketing_journey(s3_bucket, s3_path, stage_name, drop_location, src_db, src_schema)
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in sfmc_sftp_marketing_journey_file_upload delegate function")

def get_fb_api_data(params, task_instance_id,run_history_id):
    """Call fb API and download file to S3
            1. fetches API data
            2. loads JSON file into s3

    Args:
        params (dict): dictionay object with parameter details
    """
    try:
        log.info(f'Executing get_fb_api_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        page_id = params['page_id']
        metric = params['metric']
        period = params['period']
        headers = params['headers']
        request_type = params['request_type']
        raw_s3_path = params['raw_s3_path']
        output_file_name = params['output_file_name']
        raw_bucket = get_variable('raw_bucket')
        base_url = params['base_url']
        days_to_run_for = params['days_to_run_for']
        tgt_tbl = metric.upper()
        fbClientUtil = FBClientUtil().instance()

        url = f"{base_url}/{page_id}/insights"

        #archiving files
        log.info(f'Archiving files for previous {days_to_run_for} days')
        archive_params={ 
                "archive_s3_path": f"cdw/fb/archive/{metric}",
                "days_to_archive_for": days_to_run_for,
                "raw_s3_path": f"cdw/fb/{metric}/",
                "tgt_tbl": tgt_tbl
            }
        log.info('Calling archive_files()')
        archive_files(archive_params)

        # Refreshing the data for previous days_to_run_for days
        for days_to_subtract in range(0, days_to_run_for):

            start_date = (datetime.today() - timedelta(days=days_to_subtract + 1)).strftime('%Y-%m-%d')
            current_date = (datetime.today() - timedelta(days=days_to_subtract)).strftime('%Y-%m-%d')

            
            api_params = {
                'metric': metric,
                'access_token': fbClientUtil.access_token,
                'period': period,
                'start': start_date,
                'until': current_date
            }
            log.info(f'Calling the API for time period {start_date} to {current_date} for metric {metric}')
            
            response = fbClientUtil.get_data(request_type,
                                            url,
                                            headers=headers,
                                            api_params=api_params)

            file_data = None
            output_date = (datetime.today() - timedelta(days=days_to_subtract + 1)).strftime('%Y%m%d')
            s3ClientUtil = S3ClientUtil.instance()
            file_name = f'{output_date}/{output_file_name}_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.json'
            if response.status_code == 200:
                if len((eval(response.text))['data']) > 0:
                    file_data = (eval(response.text))['data'][0]
                else:
                    log.info("Received empty response")
            
            
                bytes_data = bytes(json.dumps(file_data), 'utf-8')
                
                
                log.info(f'Saving the response to S3 location {raw_bucket}/{raw_s3_path}/{metric}/{file_name}')
                s3ClientUtil.write_data(raw_bucket, f'{raw_s3_path}/{metric}/{file_name}', bytes_data)
            else:
                log.error(f"Received Response :{response.status_code}")
        
        add_file_ingestion_log(task_instance_id, tgt_tbl, f'{file_name}', run_history_id)
    
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in get_fb_api_data delegate function")

def leadgen_load_file_data(params, task_instance_id, run_history_id):
    """LeadGen domain: function to load and validate sf file data from salesforce API
           1. fetches metadata - list of files
           2. validates data and emails
           3. loads each file into s3
    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    log.info(f'Executing leadgen_load_file_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
    try:
        days_since = int(params['days_since'])
        file_prefix = params['file_prefix'] #this is used as part of the soql query
        downloaded_s3_path = params['downloaded_s3_path']
        validated_s3_path = params['validated_s3_path']
        email_dryrun = params['email_dryrun']
        tgt_tbl = params['tgt_tbl']
        encrypted_raw_bucket = get_variable('encrypted_raw_bucket')
        
        if all(v is None or '' for v in [days_since, file_prefix, downloaded_s3_path, validated_s3_path, tgt_tbl]):
            log.error(f"Required params are not available: {params}")
        
        processing_date =  (datetime.now() - timedelta(days=days_since)).date().strftime('%Y-%m-%d')

        df_records = LeadGenUtil.get_file_list_data(processing_date, file_prefix)
        if df_records.empty:
            log.warn("While trying to download, No new Lead gen files found on Sales Force portal")
            return
        else:
            s3ClientUtil = S3ClientUtil.instance()

            for row in df_records.iterrows():
                file_title = row[1]['Title']
                file_url = row[1]['VersionData']
                file_extension = row[1]['FileExtension']
                log.info("processing file: file_title: {}, file_url:{}, file_extension:{}".format(file_title, file_url, file_extension))
                response = LeadGenUtil.get_file_data(file_url)
                log.info("get_file_data response code: {} status: {}".format(response.status_code, response.reason))
                # in certain cases file extension is None
                if file_extension is None:
                    file_extension = 'xlsx'
                file_name = f'{datetime.utcnow().strftime("%Y%m%d")}/{file_title}.{file_extension}'
                s3ClientUtil.write_data(encrypted_raw_bucket, '{}/{}'.format(downloaded_s3_path, file_name), response.content)

            # finding the list of files to be processed
            # if there are any stale files (in downloaded location) from previous runs they won't be processed
            df_records['FileName'] = df_records['Title'] + "." + df_records['FileExtension']
            df_unprocessed_files = pd.DataFrame(s3ClientUtil.get_object_list(encrypted_raw_bucket, downloaded_s3_path), columns=['FileName'])
            df_records = df_unprocessed_files.merge(df_records, how='inner', left_on='FileName', right_on='FileName')
            todays_date =  f'{datetime.utcnow().strftime("%Y%m%d")}'
            log.info("Following files are identified as unprocessed files \n")
            log.info(df_records['FileName'])
            for row in df_records.iterrows():
                file_title = row[1]['Title']
                file_extension = row[1]['FileExtension']
                file_name = todays_date+"/"+row[1]['FileName']
                log.info("bucket={}, destination_path={},  source_path={}, email_dryrun={}, file_title= {}".
                                format(encrypted_raw_bucket, validated_s3_path, downloaded_s3_path, email_dryrun, row[1]['Title']))
                df_leads_file_data, validation_status = LeadGenUtil.validate_file_data(encrypted_raw_bucket, email_dryrun, row, downloaded_s3_path, file_name)
                if validation_status and df_leads_file_data is not None:
                    csv_buffer = io.StringIO()
                    df_leads_file_data.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
                    file_name = f'{datetime.utcnow().strftime("%Y%m%d")}/{file_title}_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.csv'
                    s3ClientUtil.write_data(encrypted_raw_bucket, '{}/{}'.format(validated_s3_path, file_name), csv_buffer.getvalue())
                    add_file_ingestion_log(task_instance_id, tgt_tbl, file_name, run_history_id)
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in leadgen_load_file_data delegate function")

def leadgen_load_leads_data(params, task_instance_id, run_history_id):
    """LeadGen domain: function to load and validate sf leads data from salesforce API
           1. fetches leads data
           2. validates data
           3. loads into s3
    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    log.info(f'Executing leadgen_load_leads_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
    try:
        days_since = int(params['days_since'])
        record_type_id = params['record_type_id']
        validated_s3_path = params['validated_s3_path']
        email_dryrun = params['email_dryrun']
        encrypted_raw_bucket = get_variable('encrypted_raw_bucket')
        email_environment=get_variable('ops_environment') 
        email_list = get_email_list(email_environment)
        tgt_tbl = params['tgt_tbl']
        
        if all(v is None or '' for v in [days_since, record_type_id, validated_s3_path, tgt_tbl]):
                    log.error(f"Required params are not available: {params}")
        
        processing_date = (datetime.now() - timedelta(days=days_since)).date().strftime('%Y-%m-%d')

        df_presales_leads_raw = LeadGenUtil.get_leads_data(processing_date, record_type_id)
        df_presales_leads, validation_status = LeadGenUtil.validate_and_transform_leads_data(df_presales_leads_raw, email_list, email_dryrun, email_environment)

        if validation_status and df_presales_leads is not None:
            log.info(df_presales_leads_raw.head(1))
            log.info(df_presales_leads.columns)
            # rearranging before writing csv
            df_presales_leads = df_presales_leads[
                ["Email", "First Name", "Last Name", "Campaign Name", "Campaign Date", "Club Number",
                    "Group Number", "Email Opt-in", "Birthdate", "Phone", "Address Line 1", "Address Line 2", "City",
                    "State", "Zip", "Record", "Validation"]]
            csv_buffer = io.StringIO()
            df_presales_leads.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)

            s3ClientUtil = S3ClientUtil.instance()
            # output file name's prefix is a constant
            file_name = f'{datetime.utcnow().strftime("%Y%m%d")}/lead_gen_presales_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.csv'
            s3ClientUtil.write_data(encrypted_raw_bucket, '{}/{}'.format(validated_s3_path, file_name), csv_buffer.getvalue())
            add_file_ingestion_log(task_instance_id, tgt_tbl, file_name, run_history_id)
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in leadgen_load_leads_data delegate function")


def get_fb_ad_insights_api_data(params, task_instance_id,run_history_id):
    """Call fb API and download file to S3
            1. fetches API data
            2. loads JSON file into s3

    Args:
        params (dict): dictionay object with parameter details
    """
    try:
        log.info(f'Executing get_fb_ad_insights_api_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        ad_account_id = params['ad_account_id']
        headers = params['headers']
        request_type = params['request_type']
        raw_s3_path = params['raw_s3_path']
        raw_bucket = get_variable('raw_bucket')
        base_url = params['base_url']
        tgt_tbl = (params['tgt_tbl']).split(',')
        access_token_name = params['access_token']
        days_to_run_for = params['days_to_run_for']
        archive_s3_path = params['archive_s3_path']
        s3ClientUtil = S3ClientUtil.instance()
        fbClientUtil = FBClientUtil().instance()
        access_token = fbClientUtil.get_access_token(access_token_name)
        url = f"{base_url}/{ad_account_id}/adsets"
        #archiving files
        for tbl in tgt_tbl:
            log.info(f'Archiving files for previous {days_to_run_for} days for {tbl}')
            archive_params={ 
                    "archive_s3_path": archive_s3_path,
                    "days_to_archive_for": days_to_run_for,
                    "raw_s3_path": f"{raw_s3_path}/{tbl.lower()}",
                    "tgt_tbl": tgt_tbl
                }
            log.info('Calling archive_files()')
            archive_files(archive_params)

        # Refreshing the data for previous days_to_run_for days
        for days_to_subtract in range(0, days_to_run_for):
            start_date = (datetime.today() - timedelta(days=days_to_subtract + 1)).strftime('%Y-%m-%d')
            #current_date = (datetime.today()).strftime('%Y-%m-%d')


            api_params = {
                    'access_token':access_token,
                    'limit':500,
                    'fields': "id,name,insights.filtering([{'field':'action_type','operator':'IN','value':['offsite_conversion.fb_pixel_complete_registration']}]).time_range({'since':'"+start_date+"','until':'"+start_date+"'}){spend,impressions,reach,actions,clicks},campaign{id,name,lifetime_budget}"
                }
            log.info(f'Calling the API for time period {start_date} to {start_date} for fb-ad-insight')

            
            response = fbClientUtil.get_data(request_type,
                                            url,
                                            headers=headers,
                                            api_params=api_params)

            output_date = (datetime.today() - timedelta(days=days_to_subtract + 1)).strftime('%Y%m%d')
            if response.status_code == 200:
                if len((eval(response.text))['data']) > 0:
                    data = response.text
                    fb_spends_json_str,fb_joins_json_str = FBUtil.flatten_fb_ad_insight_json(data)
                    fb_spends_bytes_data = bytes(fb_spends_json_str, 'utf-8')
                    if fb_spends_bytes_data:
                        
                        file_name = f'fb_spends_actual/{output_date}/fb_spends_actual_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.json'
                        log.info(f'Writing data to S3 path - {raw_s3_path}/{file_name}')
                        s3ClientUtil.write_data(raw_bucket, f'{raw_s3_path}/{file_name}', fb_spends_bytes_data)
                        add_file_ingestion_log(task_instance_id, tgt_tbl[0], '/'.join(file_name.split('/')[1:]), run_history_id)
                    else:
                        log.error(f'Failed to create file {file_name}')

                    fb_joins_bytes_data = bytes(fb_joins_json_str, 'utf-8')
                    if fb_joins_bytes_data:
                        
                        file_name = f'fb_joins/{output_date}/fb_joins_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.json'
                        log.info(f'Writing data to S3 path - {raw_s3_path}/{file_name}')
                        s3ClientUtil.write_data(raw_bucket, f'{raw_s3_path}/{file_name}', fb_joins_bytes_data)
                        add_file_ingestion_log(task_instance_id, tgt_tbl[1], '/'.join(file_name.split('/')[1:]), run_history_id)
                    else:
                        log.error(f'Failed to create file {file_name}')
                else:
                    log.info("Received empty response")
            else:
                log.error(f'Failed!!! with response {response.status_code}, Message: {response.content}')

    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in get_fb_ad_insights_api_data delegate function")


def get_bq_data(params, task_instance_id,run_history_id):
    """Call bq API and download file to S3
            1. fetches BigQuery data
            2. loads JSON file into s3

    Args:
        params (dict): dictionay object with parameter details
    """
    try:
        log.info(f'Executing get_bq_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')

        sql = params['sql']
        raw_s3_path = params['raw_s3_path']
        raw_bucket = get_variable('raw_bucket')
        output_file_name = params['output_file_name']
        tgt_tbl = params['tgt_tbl']

        latest_date = (datetime.today() - timedelta(days=2)).strftime("%Y%m%d")
        query = sql.format(latest_date=latest_date)

        log.info(f'Query = {query}')

        bqClientUtil = BQClientUtil().instance()
        credentials = service_account.Credentials.from_service_account_info(bqClientUtil.credentials)

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
        df = client.query(query).to_dataframe()

        if not df.empty:
            
            json_str = df.to_json(orient='records')   
            json_data = json.loads(json_str)
           
            log.info(f'Total records processed = {len(json_data)}')
            output_json_str = None
            # Converting Array of JSON to JSON records
            log.info('Converting records from JSON array to individual row')
            for elem in json_data:
                if output_json_str is not None:
                    output_json_str = output_json_str + '\n' + json.dumps(elem)
                else:
                    output_json_str = json.dumps(elem)

            output_date = (datetime.today()).strftime('%Y%m%d')
            s3ClientUtil = S3ClientUtil.instance()
            file_name = f'{output_date}/{output_file_name}_{datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]}.json'


            bytes_data = bytes(output_json_str, 'utf-8')
            log.info(f'Writing data to S3 path - {raw_s3_path}/{file_name}')
            s3ClientUtil.write_data(raw_bucket, f'{raw_s3_path}/{file_name}', bytes_data)
            add_file_ingestion_log(task_instance_id, tgt_tbl, file_name, run_history_id)

    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in get_bq_data delegate function")

def send_spends_dq_report(params, task_instance_id=None, run_history_id=None):
    """sendSpendsDQReport: function to send the spends data quality report to respective agencies

    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    try:
        log.info(f'Executing send_spends_dq_report for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        cursor = connect_to_snowflake().cursor()
        sql_params = None
    

        for agency in SPENDS_AGENCIES.values():
            spends_dq_report_sql = f"""SELECT DATE
                                        ,DOMAIN
                                        ,CHANNEL
                                        ,DESCRIPTION
                                        ,ROW_COUNT
                                        ,DATA
                                        ,INGESTION_TIMESTAMP
                                from CNTRL.SPENDS_DQ_FAILURE_LOG where AGENCY = '{agency}' and  RUN_HISTORY_ID = '{run_history_id}'""" 
            log.info(f'SQL for DQ report: {spends_dq_report_sql}')
            spends_dq_report_result = execute_commands(cursor,spends_dq_report_sql,sql_params)                                

            spends_dq_columns = ['DATE'
                                ,'DOMAIN'
                                ,'CHANNEL'
                                ,'DESCRIPTION'
                                ,'ROW_COUNT'
                                ,'DATA'
                                ,'INGESTION_TIMESTAMP']
            spends_dq_report_dataframe = pd.DataFrame(spends_dq_report_result,columns=spends_dq_columns)
            spends_dq_report_dataframe.set_index("DATE",inplace=True)
            now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
            today_date = datetime.now().strftime("%m-%d-%Y")
            writer = pd.ExcelWriter("/tmp/{agency}_DQ_Report_".format(agency=agency)+now+".xlsx",engine='xlsxwriter')

            workbook  = writer.book
            header_format = workbook.add_format({
                        'bold': True,
                        'text_wrap': True,
                        'valign': 'top',
                        'fg_color': '#1E90FF',
                        'font_color':'#FFFFFF',
                        'border': 1}) 
            log.info(f'DQ report size: {spends_dq_report_dataframe.size}')
            # Excel Sheet for Team Lift DQ Report: Write the DQ sheet only when there are results
            if spends_dq_report_dataframe.size > 0:
                spends_dq_report_dataframe.to_excel(excel_writer = writer,sheet_name='DQ Report',startrow=1, header=False)
                worksheet = writer.sheets['DQ Report'] 
                # Write the column headers with the defined format
                for col_num, value in enumerate(spends_dq_columns):
                    worksheet.write(0, col_num, value, header_format)
                writer.save()        
                email_list = get_agency_email_list(agency)
                log.info("Function send_spends_dq_report: Sending report email to",email_list)     

                # Send email to respective agencies
                send_email(
                to=email_list,
                subject='{agency} DQ Report {today_date}'.format(agency=agency,today_date=today_date),  
                html_content='Hi Team,<br><br>Please find the DQ report for {today_date}.<br><br>Thanks'.format(today_date=today_date),          
                files = ['/tmp/{agency}_DQ_Report_'.format(agency=agency)+now+'.xlsx']
                )
             
        error_count_except_dma_check = f"""SELECT count(*) from CNTRL.SPENDS_DQ_FAILURE_LOG 
            where RUN_HISTORY_ID = '{run_history_id}' and FAILED_TEST <> 'Incorrect DMA'"""            
        log.info(f'SQL for error count except DMA check: {error_count_except_dma_check}')                                                                                   
        error_count_result = execute_commands(cursor,error_count_except_dma_check,sql_params)

        if error_count_result[0][0] > 0:
            log.error(f'Result for error count except DMA check: {error_count_result}')
            raise Exception(f"DQ checks(except DMA checks) is failed")
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in send_spends_dq_report delegate function")

def club_demographics_load_file_data(params, task_instance_id, run_history_id):
    """Reads the given src files and uploads it to sftp location for club demographics
        in csv format
    Args:
        params (dict): dictionary object with paramter details
        task_instance_id (str): ingested variable based on the task instance id created
        run_history_id (str): ingested variable based on the run history id provided from airflow
    """
    try:
        log.info(f'Executing club_demographics_load_file_data for task_instance_id:{task_instance_id} and run_history_id:{run_history_id}')
        log.info(params)
        file_read_locations = params['file_read_locations']
        drop_location = params['drop_location']
        domain = params['domain']
        if all(v is None or '' for v in [file_read_locations, drop_location, domain]):
            log.error(f"Required params are not available: {params}")

        club_demographics = SFTPClubDemographics(domain)
        club_demographics.read_write_sftp_data(file_read_locations, drop_location)
        
    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in club_demographics_load_file_data delegate function")