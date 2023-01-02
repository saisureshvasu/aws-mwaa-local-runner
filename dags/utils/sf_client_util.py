import logging
import json
import urllib3
urllib3.disable_warnings()
log = logging.getLogger(__name__)
from simple_salesforce import Salesforce
from utils.parse_variable import get_variable
from utils.sm_util import SMUtil
import os
import pandas as pd

SM_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/salesforce_sm'.format(get_variable("env"))

class SFClientUtil:

    _instance = None
    
    
    def __init__(self, local_credentials):
        """
        constructor function for all salesforce api requests. initializes sf_client attribute

        Args:
            local_credentials (bool)
        """
        if local_credentials:
            log.info("initializing SFClientUtil from local credentials")
            working_dir = os.environ.get('AIRFLOW_WORKING_DIR')  
            # Have this env variable created on your local machine to test locally
            # and keep credentials json file
            login_info = json.load(open(working_dir + 'sf_login.json'))
            username = login_info['username']
            password = login_info['password']
            security_token = login_info['security_token']
            domain = login_info['domain']
            instance_url = login_info['instance_url']
            self.sf_client =  Salesforce(instance_url=instance_url, username=username, password=password,
                                security_token=security_token,
                                domain=domain)
        else:
            log.info("initializing SFClientUtil from Secret Manager credentials")
            smUtil = SMUtil.instance()
            log.info(f"reading Secret Manager credentials for {SM_SECRET_ID_NAME}")
            login_info = smUtil.read_from_aws_sm_fn(SM_SECRET_ID_NAME)
            if login_info:
                login_info = json.loads(login_info)
                self.sf_client =  Salesforce(instance_url=login_info['instance_url'], username=login_info['username'], password=login_info['password'],
                                    security_token=login_info['security_token'],
                                    domain=login_info['domain'])
                log.info("sf_client initialized")
                log.info(self.sf_client)
            else:
                log.info("sf_client cannot be initialized")
                raise Exception("Credentails unavailable from Secret Manager")

    @staticmethod
    def instance(local_credentials=False):
        """
        static method to support singleton client objects

        Args:
            local_credentials (bool, optional): Defaults to False.

        Returns:
            SFClientUtil
        """
        if SFClientUtil._instance is None:
            SFClientUtil._instance = SFClientUtil(local_credentials)
        return SFClientUtil._instance

    
    def sf_query(self, querystring):
        """
        sf_query is utility function to make any salesforce queries 

        Args:
            querystring (str): salesforce query

        Returns:
            dataframe
        """
        log.info("salesforce authentication: {}".format('success' if self.sf_client is not None else 'failed'))
        try:
            response = self.sf_client.query(querystring)
            log.info("salesforce query: {}".format(querystring))
            list_records = response.get('records')
            next_records_url = response.get('nextRecordsUrl')

            while not response.get('done'):
                response = self.sf_client.query_more(next_records_url, identifier_is_url=True)
                list_records.extend(response.get('records'))
                next_records_url = response.get('nextRecordsUrl')
            return pd.DataFrame(list_records)
        except Exception as ex:
            log.error(ex)
            raise Exception("Salesforce soql query failed")