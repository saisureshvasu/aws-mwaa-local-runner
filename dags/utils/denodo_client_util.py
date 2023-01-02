import requests
import logging
import json
import urllib3
urllib3.disable_warnings()
log = logging.getLogger(__name__)
from utils.mock_endpoints import *
from utils.sm_util import SMUtil
from utils.parse_variable import get_variable
import smbclient
import pyodbc

SM_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/denodo_sm'.format(get_variable("env"))




def denodo_api_get(url, params={}):
    """
    denodo_api_get is utility function for all HTTP get requests

    Args:
        url (string): url endpoint
        params (dict, optional): Defaults to {}.

    Raises:
        Exception: None

    Returns:
        Response : response consists of all jobs
    """    

    # Get base64str and base url 
    sm_util = SMUtil.instance()
    response_base = sm_util.get_base64str_and_url(SM_SECRET_ID_NAME)
    base64str,  base_url = response_base[0], response_base[1]

    # Form Endpoint
    endpoint =  "https://" + base_url + url    
    log.info("endpoint: {}".format(endpoint))
    
    # Requesting to get response 
    response = requests.get(endpoint, params=params, headers={'Authorization': 'Basic '+base64str}, verify=False)
    log.info("response status code: {}".format(response.status_code))
    json_response = {}

    if (response.status_code == 200):
        json_response = response.json()
        # log.info(json_response) # disabling this log since it causes massive log output
    else:
        raise Exception(f"Denodo API invocation error. Received status code: {response.status_code}")
    return json_response


def denodo_api_put(url, params={}, data={}):
    """
    denodo_api_put is utility function for all HTTP put requests

    Args:
        url (string): URL endpoint
        params (dict, optional): API parameters Defaults to {}.
        data (dict, optional): API payload. Defaults to {}.

    Raises:
        Exception: None
    """    
    # Get base64str and base url 
    sm_util = SMUtil.instance()
    response_base = sm_util.get_base64str_and_url(SM_SECRET_ID_NAME)
    base64str,  base_url = response_base[0], response_base[1]

    # Form Endpoint
    endpoint =  "https://" + base_url + url    
    log.info("endpoint: {}".format(endpoint))
    
    # Requesting to get response 
    response = requests.put(endpoint, params=params, headers={'Authorization': 'Basic '+base64str}, verify=False, data=data)
    log.info("response status code: {}".format(response.status_code))

    if not(response.status_code == 200 or response.status_code == 204):
        raise Exception("Denodo API invocation error. Received status code: {}".format(response.status_code))


def denodo_cache_refresh(jobId, projectId):
    """
    denodo_cache_refresh performs action to start when the state is NOT_RUNNING

    Args:
        jobId (string): _description_
        projectId (string): _description_

    Raises:
        ex: None
    """    
    try:
        endpoint = '/webadmin/denodo-scheduler-admin/public/api/projects/{projectId}/jobs/{jobId}/status?uri=//localhost:8000'.format(projectId = projectId, jobId = jobId)
        
        # Get the reponse to check whether state is running or not
        response = denodo_api_get(endpoint)
        log.info("Current job status: {}".format(response))

        # Check whether the state is NOT_RUNNING 
        if response["state"] == 'NOT_RUNNING':
            # Change the job status by starting
            log.info("Performing Refresh")
            denodo_api_put(endpoint, data=json.dumps({"action":"start"}))
        else:
            log.warn("Refresh skipped as job is in {}".format(response["state"]))
    except Exception as ex:
        log.error("Error while performing denodo cache refresh")
        raise ex


def denodo_fetch_scheduler_jobs():
    """
    denodo_fetch_scheduler_jobs is to fetch all jobs using denodo_api_get

    Raises:
        ex: None

    Returns:
        Response obj: response consists of all jobs
    """    
    try:
        # Get reponse to fetch all jobs status
        response = denodo_api_get('/webadmin/denodo-scheduler-admin/api/jobs?uri=//localhost:8000')
        return response
    except Exception as ex:
        log.error("Error while fetching denodo scheduled jobs")
        raise ex    


# -----------------------------------------------------------------------------------------------------------
#   Purpose: 
#   Exception: NONE
#   Return: views_to_be_refreshed
# -----------------------------------------------------------------------------------------------------------


def denodo_fetch_object_dependency(db, schema, table):
    """
    denodo_fetch_object_dependency is to get response which consists of views to be refreshed

    Args:
        db (string): Database name
        schema (string): schema name
        table (string): table name

    Raises:
        ex: None

    Returns:
        list : views to be refreshed
    """    
    try:
        # fetch obj dependecy response
        response = denodo_api_get('/server/unified_analytics_reporting/SourceTableLookupforViewsDerivedFromEDWandSnowflake/views/rest_Source_Table_Lookup_for_Views_Derived_From_EDW_and_Snowflake')
        log.info(len(response["elements"]))

        # extract the elements from the response 
        elements = response["elements"]
        
        # Get the views_to_be_refreshed from elements
        views_to_be_refreshed = set()
        for element in elements:
            if element["source_catalog_name"] == db and element["source_schema_name"] == schema and element["source_table_name"] == table:
                views_to_be_refreshed.add(element["view_name"])
        log.info(views_to_be_refreshed)
        return views_to_be_refreshed
    except Exception as ex:
        log.error("Error while fetching the object dependency ")
        raise ex


def denodo_fetch_object_dependency():
    """
        denodo_fetch_object_dependency returns the complete list of all views and tables

    Raises:
        ex: None

    Returns:
        list : views to be refreshed
    """    
    try:
        # fetch obj dependecy response
        response = denodo_api_get('/server/unified_analytics_reporting/SourceTableLookupforViewsDerivedFromEDWandSnowflake/views/rest_Source_Table_Lookup_for_Views_Derived_From_EDW_and_Snowflake')
        log.info(len(response["elements"]))

        return response["elements"]
    except Exception as ex:
        log.error("Error while fetching the object dependency ")
        raise ex


def test_file():
   
    try :
        fd = smbclient.open_file(r"\\pfinfodev01\SourceFiles\TM1\TM1_Admin_Module.csv", mode="r")
        data = fd.read()
        print(data)
    except Exception as ex:
        print(ex)


    try :
        fd = smbclient.open_file(r"\\10.50.32.5\SourceFiles\TM1\TM1_Admin_Module.csv", mode="r")
        data = fd.read()
        print(data)
    except Exception as ex:
        print(ex)

def test_odbc():
    smUtil = SMUtil.instance()
    login_info = smUtil.read_from_aws_sm_fn("pf-aas-airflow-nonprod/connections/outbound_sm")
    """converting string to dict for accessing the keys and values if the login_info is not null
    """
    if (login_info):
        login_info = json.loads(login_info)
        server,  username, password = login_info['server'], login_info['r_username'], login_info['r_password']
    
    database = 'PF_CCM_Live' 
    print([x for x in pyodbc.drivers() if x.endswith(' for SQL Server')])
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+ password+';Trusted_Connection=no', autocommit=True)
    cnxn.set_attr(pyodbc.SQL_ATTR_TXN_ISOLATION, pyodbc.SQL_TXN_READ_UNCOMMITTED)
    # changing the isolation level because default is not READ UNCOMMITTED
    # https://github.com/mkleehammer/pyodbc/wiki/Database-Transaction-Management
    # https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-sessions-transact-sql
    # For windows authentication you need to put Trusted_Connection=yes; and for password authentication it needs to be false
    cursor = cnxn.cursor()

    cursor.execute("""SELECT top 3
            F1.ABCID AS CLUBID
            , F1.WEBPAYMENTPLANID AS PlanID
            , F1.MANDATORYOFFER
            , F1.STARTUPFEE
            , F1.MONTHLYFEE
            , F1.YEARLYFEE
            , F1.MEMBERSHIPTYPEID
            , SUBSTRING( F1.ABCTITLE, 1, 200) AS ABCTitle
            , F1.UPGRADEOFFER
            , F1.PROPOSED_SPECIALOFFER
            , F1.PFDELETED
            , F1.ABCDELETED
            , F1.APPROVALSTATUS
        FROM FMT F1""") 
    row = cursor.fetchone() 
    while row: 
        print(row[0])
        row = cursor.fetchone()
    cursor.close()
    cnxn.close()