import requests
import logging
log = logging.getLogger(__name__)
from utils.sm_util import SMUtil
from utils.parse_variable import get_variable
import json

SM_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/bq_sm'.format(get_variable("env"))

class BQClientUtil:

    _instance = None

    def __init__(self):
        """
        constructor function for all bq requests. initializes access_token attribute
        """        
        smUtil = SMUtil.instance()
        auth_info = smUtil.read_from_aws_sm_fn(SM_SECRET_ID_NAME)
        self.credentials = json.loads(auth_info)

        
    @staticmethod
    def instance():
        """
        static method to support singleton client objects

        Returns:
            SFClientUtil
        """
        if BQClientUtil._instance is None:
            BQClientUtil._instance = BQClientUtil()
        return BQClientUtil._instance
    
     

