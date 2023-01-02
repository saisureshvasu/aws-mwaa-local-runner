import requests
import logging
log = logging.getLogger(__name__)
from utils.sm_util import SMUtil
from utils.parse_variable import get_variable

SM_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/fb_sm'.format(get_variable("env"))

class FBClientUtil:

    _instance = None

    def __init__(self):
        """
        constructor function for all fb api requests. initializes access_token attribute
        """        
        smUtil = SMUtil.instance()
        auth_info = smUtil.read_from_aws_sm_fn(SM_SECRET_ID_NAME)
        self.access_token = auth_info

        
    @staticmethod
    def instance():
        """
        static method to support singleton client objects

        Returns:
            SFClientUtil
        """
        if FBClientUtil._instance is None:
            FBClientUtil._instance = FBClientUtil()
        return FBClientUtil._instance
    
    # -----------------------------------------------------------------------------------------------------------
    #   Purpose: 
    #   Exception: NONE
    #   Return: response
    # ----------------------------------------------------------------------------------------------------------- 
    def get_data(self,  request_type, url, headers, api_params):
        """
        generic method to support HTTP GET requests to API

        Args:
            request_type (string): API request type (GET/POST)
            url (string): API URL
            headers (string): API header parameters
            api_params (dict): API parameters

        Returns:
            response: response object
        """
        try:
            response = requests.request(request_type,
            url,
            headers=eval(headers),
            params=api_params)
            log.info(f'response is :{response}')
            return response
        except Exception as ex:
            log.error(f"Failed to connect to API: {ex}")
            raise

    def get_access_token(self, access_token_name):
        smUtil = SMUtil.instance()
        auth_info = smUtil.read_from_aws_sm_fn(access_token_name)
        return auth_info        

