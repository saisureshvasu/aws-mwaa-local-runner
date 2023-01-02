import paramiko
import logging
log = logging.getLogger(__name__)
logging.getLogger("paramiko").setLevel(logging.INFO)
from utils.sm_util import SMUtil
from utils.parse_variable import get_variable
import json


class SFTPClientUtil:

    _instance = None

    def __init__(self, secret=None):
        """
        constructor function for all ftp api requests. initializes variables needed for ftp connection
        """ 
        try:
            if (secret is not None):
                smUtil = SMUtil.instance()
                login_info = smUtil.read_from_aws_sm_fn(secret)
                """converting string to dict for accessing the keys and values if the login_info is not null
                """
                if (login_info):
                    login_info = json.loads(login_info)
                    self.host, self.port, self.username, self.password = login_info['host'], login_info['port'], login_info['username'], login_info['password']
                    log.info(f'values retried {self.host}')
        except Exception as ex:
            print(f"{type(ex).__name__} {__file__}  {ex.__traceback__.tb_lineno}")
            raise ex

        
    @staticmethod
    def instance(secret=None):
        """
        static method to support singleton client objects

        Returns:
            SFTPClientUtil
        """
        if SFTPClientUtil._instance is None:
            SFTPClientUtil._instance = SFTPClientUtil(secret)
        return SFTPClientUtil._instance
    

    # -----------------------------------------------------------------------------------------------------------
    #   Purpose: 
    #   Exception: NONE
    #   Return: sftp
    # ----------------------------------------------------------------------------------------------------------- 
    def open_connection(self):
        """
        generic method to open sftp connections

        Args:
            None

        Returns:
            sftp_session: a new paramiko SFTPClient object, referring to an sftp session (channel) across the transport
        """
        try:
            #port should be integer
            
            transport = paramiko.Transport((self.host,int(self.port)))
            transport.connect(None, self.username, self.password)
            sftp_session = paramiko.SFTPClient.from_transport(transport)
            if sftp_session:
                log.info(f"sftp session created successfully to server {self.host}")
            return sftp_session
        except Exception as ex:
            log.error(f"Failed to connect to sftp: {ex}")

    def close_connection(self, sftp):
        """
        generic method to close sftp connections

        Args:
            sftp (obj): paramiko SFTPClient session object
        """
        try:
           if sftp: sftp.close()
           #if transport: transport.close() #todo: is this needed
        except Exception as ex:
            log.error(f"sftp session (channel) already closed: {ex}")

    

