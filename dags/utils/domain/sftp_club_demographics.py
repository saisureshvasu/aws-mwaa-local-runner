from utils.domain.sftp_source import SFTPSource
from datetime import date
import logging

log = logging.getLogger(__name__)

class SFTPClubDemographics(SFTPSource):
    def __init__(self, domain):
        super(SFTPClubDemographics, self).__init__(domain)

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