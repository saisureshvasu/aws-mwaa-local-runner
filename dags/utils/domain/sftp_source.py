from utils.sftp_client_util import SFTPClientUtil
from utils.s3_client_util import S3ClientUtil
from utils.constants import get_sm_secret_id
from io import BytesIO
from datetime import date
import logging

log = logging.getLogger(__name__)
class SFTPSource:
    
    sftp_client, S3ClientUtil = None, None
    def __init__(self, domain):
        # Establising sftp connection
        if self.sftp_client is None:
            secret_name = get_sm_secret_id(domain)
            stfp_client_util = SFTPClientUtil.instance(secret_name)
            self.sftp_client = stfp_client_util.open_connection()
        # Establising s3 client connection
        if self.S3ClientUtil is None:
            self.S3ClientUtil = S3ClientUtil.instance()
    
    def get_sftp_read_write_locations(self, file_read_locations, drop_location):
        return self.value

    def read_write_sftp_data(self, file_read_locations, drop_location):
        
        try:
            file_read_write_locations, s3_bucket = self.get_sftp_read_write_locations(file_read_locations, drop_location)  
            for file_read_write_location in file_read_write_locations:
                file_read_location, file_write_location = file_read_write_location[0], file_read_write_location[1]
                # Read file
                file = self.sftp_client.open(file_read_location,'r',100)
                file_data = file.read()

                while file_data:
                    # Write file
                    self.S3ClientUtil.upload_fileobj(BytesIO(file_data), s3_bucket, file_write_location)
                    log.info("Reading file...")
                    file_data = file.read()
                log.info("Data upload successful")
            return 
        except Exception as ex:
            log.error(f"Exception in read_write_sftp_data: {ex}")
            log.error(f"{type(ex).__name__} {__file__} {ex.__traceback__.tb_lineno}")
            raise ex
        finally:
            file.close()
            self.sftp_client.close()        


class SFTPClubDemographics(SFTPSource):
    def __init__(self, domain):
        super().__init__(domain)

    def get_sftp_read_write_locations(self, file_read_locations, drop_location):

        try:
            # Current date
            today = date.today()
            log.info(f"Current Date: {today}")

            # TODO: replace it with logic
            file_usa_suffix = ['_1208190140.txt', '_TEST', '_1126190316.txt']
            file_can_suffix = ['_1208190101.txt', '_1126190304.txt']

            # S3 bucket
            s3_bucket = drop_location.split("/")[0]
            log.info(f"s3_bucket: {s3_bucket}")

            file_read_write_locations = []
            for file_location in file_read_locations:
                
                # Choose the file suffix
                if file_read_location[-3:] == 'USA':
                    file_suffixes = file_usa_suffix
                elif file_read_location[-3:] == 'CAN':
                    file_suffixes = file_can_suffix
                    
                for file_suffix in file_suffixes:

                    # File read location
                    file_read_location = file_location +  file_suffix
                    log.info(f"file_read_location: {file_read_location}")

                    # File write location
                    file_write_location = "/".join(drop_location.split("/")[1:]) + today.strftime('%Y-%m-%d') + '/' + file_read_location.split('/')[-1]
                    log.info(f"file_destination: {file_write_location}")

                    # Append read and write location as tuple: [(read_location_1, write_location_1), (read_location_2, write_location_2)]
                    file_read_write_locations.append((file_read_location, file_write_location))
                    log.info(f"file_read_write_locations: {file_read_write_locations}")

            return file_read_write_locations, s3_bucket
            
        except Exception as ex:
            log.error(f"Exception in get_sftp_read_write_locations: {ex}")
            log.error(f"{type(ex).__name__} {__file__} {ex.__traceback__.tb_lineno}")
            raise ex