import logging
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
from datetime import datetime

log = logging.getLogger(__name__)

class ServicesUtil:

    @staticmethod
    def upload_sf_to_s3(s3_path, file_name, stage_name, src_tbl, src_db, src_schema):
        cursor = None

        try:
            cursor = connect_to_snowflake().cursor()
            
            sql_command = f"""copy into @{src_db}.{src_schema}.{stage_name}/{s3_path}{file_name}
                    from (
                        select 
                            t1.*
                        from {src_db}.{src_schema}.{src_tbl} t1
                    )
                    file_format = (type = csv compression = none field_optionally_enclosed_by='"' null_if=''  )
                    overwrite = true
                    single = true
                header = true"""
            sql_params = []
            result = execute_commands(cursor, sql_command, sql_params)
            log.info("copy into performed")
            log.info(f"sql: {sql_command}")
            log.info(result)

            if result[0][0]==0:
                log.error(f'Exiting s3 upload process: rows unloaded is {result[0][0]}')
                raise Exception(f'{src_tbl} is empty; Exiting s3 upload process') 
                
        except Exception as ex:
            log.error(f"Exception  in fetch_hssp_marketing_journey: {ex}")
            log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
            raise ex
        finally:
            # Close
            if cursor:
                cursor.close()
    
    # TODO: refactor later to utilize a single method
    
    @staticmethod
    def upload_sf_member_type_dim_to_s3(s3_path, file_name, stage_name, src_tbl, src_db, src_schema):
        cursor = None
        try:
            cursor = connect_to_snowflake().cursor()
            
            sql_command = f"""copy into @{src_db}.{src_schema}.{stage_name}/{s3_path}{file_name}
                    from (
                        select 
                            MEMBER_TYPE
                            , EXPERIENCE as DIGITAL_EXPERIENCE
                            , IS_PREPAID
                            , MEMBER_CATEGORY
                            , SERVICES_ELIGIBILITY_CATEGORY
                        from {src_db}.{src_schema}.{src_tbl}
                    )
                    file_format = (type = csv compression = none field_optionally_enclosed_by='"' null_if=''  )
                    overwrite = true
                    single = true
                header = true"""
            sql_params = []
            result = execute_commands(cursor, sql_command, sql_params)
            log.info("copy into performed")
            log.info(f"sql: {sql_command}")
            log.info(result)

            if result[0][0]==0:
                log.error(f'Exiting s3 upload process: rows unloaded is {result[0][0]}')
                raise Exception(f'{src_tbl} is empty; Exiting s3 upload process') 
                
        except Exception as ex:
            log.error(f"Exception  in fetch_hssp_marketing_journey: {ex}")
            log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
            raise ex
        finally:
            # Close
            if cursor:
                cursor.close()

    @staticmethod
    def upload_sf_crowd_meter_to_s3(s3_path, file_name, stage_name, src_tbl, src_db, src_schema):
        cursor = None
        try:
            cursor = connect_to_snowflake().cursor()
            
            sql_command = f"""copy into @{src_db}.{src_schema}.{stage_name}/{s3_path}{file_name}
                    from (
                        select 
                            CURRENT_CLUB_NUMBER_POS
                            , DAY_LONG_DESC
                            , HOUR24
                            , VISIT_COUNT
                        from {src_db}.{src_schema}.{src_tbl}
                    )
                    file_format = (type = csv compression = none field_optionally_enclosed_by='"' null_if=''  )
                    overwrite = true
                    single = true
                header = true"""
            sql_params = []
            result = execute_commands(cursor, sql_command, sql_params)
            log.info("copy into performed")
            log.info(f"sql: {sql_command}")
            log.info(result)

            if result[0][0]==0:
                log.error(f'Exiting s3 upload process: rows unloaded is {result[0][0]}')
                raise Exception(f'{src_tbl} is empty; Exiting s3 upload process') 
                
        except Exception as ex:
            log.error(f"Exception  in fetch_hssp_marketing_journey: {ex}")
            log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
            raise ex
        finally:
            # Close
            if cursor:
                cursor.close()