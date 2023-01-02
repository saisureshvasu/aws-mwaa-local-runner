import requests
import logging
log = logging.getLogger(__name__)
from utils.sm_util import SMUtil
from utils.parse_variable import get_variable

SM_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/cms_sm'.format(get_variable("env"))

class CMSClientUtil:

    _instance = None

    def __init__(self):
        """
        constructor function for all cms api requests. initializes access_token attribute
        """        
        smUtil = SMUtil.instance()
        auth_info = smUtil.read_from_aws_sm_fn(SM_SECRET_ID_NAME)
        self.access_token = auth_info

        
    @staticmethod
    def instance():
        """
        static method to support singleton client objects

        Returns:
            CMSClientUtil: CMSClientUtil object
        """        
        if CMSClientUtil._instance is None:
            CMSClientUtil._instance = CMSClientUtil()
        return CMSClientUtil._instance
    
    # -----------------------------------------------------------------------------------------------------------
    #   Purpose: generic method to support HTTP GET requests to API
    #   Exception: NONE
    #   Return: response
    # ----------------------------------------------------------------------------------------------------------- 
    def get_data(self,  request_type, url, headers, api_params):
        """
         generic method to support HTTP GET requests to API

        Args:
            request_type (string): HTTP request type(GET/POST)
            url (string): API URL
            headers (string): API header paramenters
            api_params (dict): parameters to send in API call

        Returns:
            Response: API response object
        """        
        try:
            if len(headers)>0:
                response = requests.request(request_type, url, headers=eval(headers), params=api_params)
            else:
                response = requests.request(request_type, url, params=api_params)
            log.info(f'response is :{response}')
            return response
        except Exception as ex:
            log.error(f"Failed to connect to API: {ex}")

    

