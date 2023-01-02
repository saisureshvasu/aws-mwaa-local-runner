from datetime import datetime
from utils.s3_client_util import S3ClientUtil
from utils.sftp_client_util import SFTPClientUtil
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import logging

log = logging.getLogger(__name__)

class SFMCUtil:
   
    @staticmethod
    def upload_hssp_marketing_journey(s3_bucket, s3_path, stage_name, drop_location, src_db, src_schema):
        cursor = None
        sftp_session = None
        try:

            file_name = 'HSSP_Daily_Update_'+datetime.utcnow().strftime('%Y%m%d')+'.csv'
            cursor = connect_to_snowflake().cursor()
            sql_params = []
            sql_command = f"""select exists(
                                select * 
                                from {src_db}.information_schema.tables 
                                where 
                                table_schema = '{src_schema}' and 
                                table_name = 'HSSP_MARKETING_JOURNEY_PREV'
                            )"""
            result = execute_commands(cursor, sql_command, sql_params)
            log.info(f"checking for prev table: {sql_command}")
            log.info(result[0][0])

            # Note that the copy into doesn't include all columns from the table like SID, TEEN_AMPERITY_ID and ROW_HASH
            # Those are needed to provide uniqueness and debugging
            if str(result[0][0]) == 'True':
                sql_command = f"""copy into @{src_db}.{src_schema}.{stage_name}/{s3_path}{file_name}
                        from (
                select 
                    AMPERITY_ID
                    , CURRENT_MEMBER_TYPE
                    , CURRENT_STATUS_CODE
                    , EMAIL_OPT_IN
                    , FIRST_CHECKIN_DATE_AFTER_REGISTRATION
                    , FIRST_JOIN_DATE_AFTER_HSSP
                    , FIRST_USED_ON_DATE
                    , HS_CITY
                    , HS_NAME
                    , HS_STATE
                    , HSSP_ADA_NAME
                    , HSSP_ADA_STATUS
                    , HSSP_CLASS_YEAR
                    , HSSP_CLUB_CLOSE_DATE
                    , HSSP_CLUB_COUNTRY
                    , HSSP_CLUB_DESC
                    , HSSP_CLUB_NUMBER_PF
                    , HSSP_CLUB_NUMBER_PF_AND_DESC
                    , HSSP_CLUB_NUMBER_POS
                    , HSSP_CLUB_NUMBER_POS_AND_DESC
                    , HSSP_CLUB_OPEN_DATE
                    , HSSP_CLUB_STATE
                    , HSSP_CLUB_STATUS
                    , HSSP_CORPORATE_CLUB_FLAG
                    , HSSP_CORPORATE_REGION
                    , HSSP_DMA
                    , HSSP_FBC_NAME
                    , HSSP_HIGH_LEVEL_REGION
                    , HSSP_MARKETING_REGION
                    , HSSP_OPERATIONS_DISTRICT
                    , HSSP_OPS_REGION
                    , HSSP_OWNER_GROUP_NUMBER
                    , HSSP_OWNER_GROUP_NUMBER_DESC
                    , HSSP_PHYSICAL_PRESALES_DATE
                    , HSSP_PRESALES_DATE
                    , HSSP_PROJECTED_OPEN_DATE
                    , HSSP_REPORTING_REGION
                    , HSSP_SSS_CLASS_YEAR
                    , HSSP_SSS_COMP_INDICATOR
                    , HSSP_SSS_REGION
                    , HSSP_STRATEGY_REPORTING_REGION
                    , IS_ACTIVE
                    , IS_MOBILE_APP_USER
                    , LAST_CANCEL_DATE
                    , LAST_CHECKIN_DATE_AFTER_REGISTRATION
                    , LAST_JOIN_DATE
                    , LAST_JOIN_DATE_AFTER_HSSP
                    , LAST_PG_DAYPASS_REDEMPTION_DATE_AFTER_REGISTRATION
                    , LAST_USED_ON_DATE
                    , PARENT_TEEN_HAS_CHECKED_IN_FLAG
                    , PFX_USER_CREATION_DATE
                    , PFX_USER_ID
                    , PG_ADDRESS
                    , PG_ADDRESS_2
                    , PG_AMPERITY_ID
                    , PG_BIRTH_DATE
                    , PG_CITY
                    , PG_COUNTRY
                    , PG_CURRENT_MEMBER
                    , PG_CURRENT_MEMBER_CLASSIFICATION
                    , PG_EMAIL
                    , PG_EMAIL_OPT_IN
                    , PG_GENDER
                    , PG_MOBILE
                    , PG_NAME
                    , PG_NAME_LAST
                    , PG_SMS_OPT_IN
                    , PG_STATE
                    , PG_ZIP
                    , RECORD_TYPE
                    , REPORT_CACHE_DATETIME
                    , REPORT_CACHE_DATETIME_FORMATTED
                    , REPORT_DATE
                    , TEEN_2019_REPEAT_FLAG
                    , TEEN_ADDRESS_1
                    , TEEN_ADDRESS_2
                    , TEEN_AGE_AT_REGISTRATION
                    , TEEN_AGREEMENT_ENTRY_SOURCE
                    , TEEN_BARCODE
                    , TEEN_BIRTH_DATE
                    , TEEN_CANCEL_DATE
                    , TEEN_CITY
                    , TEEN_CLUB_NUMBER
                    , TEEN_COUNTRY
                    , TEEN_DELETE_CCPA
                    , TEEN_DELETE_GDPR
                    , TEEN_DELETE_OTHER
                    , TEEN_EMAIL_ADDRESS
                    , TEEN_EMAIL_OPT_IN
                    , TEEN_FIRST_NAME
                    , TEEN_GENDER
                    , TEEN_GROUP_NAME
                    , TEEN_IS_ACTIVE
                    , TEEN_IS_BILLING
                    , TEEN_LAST_NAME
                    , TEEN_M_ID
                    , TEEN_MARKETING_OPT_IN
                    , TEEN_MEMBER_CLASSIFICATION_AT_SIGN_UP
                    , TEEN_MEMBER_NUMBER_UNIQUE
                    , TEEN_MEMBER_SID
                    , TEEN_MEMBER_TYPE
                    , TEEN_OPT_IN_CCPA
                    , TEEN_OPT_IN_GDPR
                    , TEEN_OPT_IN_OTHER
                    , TEEN_PARENT_FLAG
                    , TEEN_PARENT_SAME_HOUSEHOLD_FLAG
                    , TEEN_PF_JOIN_DATE
                    , TEEN_PRIMARY_PHONE
                    , TEEN_PROMOTION
                    , TEEN_PROSPECT_NUMBER_UNIQUE
                    , TEEN_PROSPECT_SID
                    , TEEN_REGISTRATION_DATE
                    , TEEN_SMS_OPT_IN
                    , TEEN_SOURCE
                    , TEEN_STATE
                    , TEEN_ZIP_CODE
                    , TOTAL_HSSP_VISIT_COUNT 
                from (
                    select t1.* from (
                      select 
                        hash(new.AMPERITY_ID, new.TEEN_AMPERITY_ID, new.TEEN_CLUB_NUMBER, new.TEEN_PARENT_FLAG) as HSSP_MARKETING_JOURNEY_SID
                        , new.*
                        , hash(
                            new.AMPERITY_ID
                            , new.CURRENT_MEMBER_TYPE
                            , new.CURRENT_STATUS_CODE
                            , new.EMAIL_OPT_IN
                            , new.FIRST_CHECKIN_DATE_AFTER_REGISTRATION
                            , new.FIRST_JOIN_DATE_AFTER_HSSP
                            , new.FIRST_USED_ON_DATE
                            , new.HS_CITY
                            , new.HS_NAME
                            , new.HS_STATE
                            , new.HSSP_ADA_NAME
                            , new.HSSP_ADA_STATUS
                            , new.HSSP_CLASS_YEAR
                            , new.HSSP_CLUB_CLOSE_DATE
                            , new.HSSP_CLUB_COUNTRY
                            , new.HSSP_CLUB_DESC
                            , new.HSSP_CLUB_NUMBER_PF
                            , new.HSSP_CLUB_NUMBER_PF_AND_DESC
                            , new.HSSP_CLUB_NUMBER_POS
                            , new.HSSP_CLUB_NUMBER_POS_AND_DESC
                            , new.HSSP_CLUB_OPEN_DATE
                            , new.HSSP_CLUB_STATE
                            , new.HSSP_CLUB_STATUS
                            , new.HSSP_CORPORATE_CLUB_FLAG
                            , new.HSSP_CORPORATE_REGION
                            , new.HSSP_DMA
                            , new.HSSP_FBC_NAME
                            , new.HSSP_HIGH_LEVEL_REGION
                            , new.HSSP_MARKETING_REGION
                            , new.HSSP_OPERATIONS_DISTRICT
                            , new.HSSP_OPS_REGION
                            , new.HSSP_OWNER_GROUP_NUMBER
                            , new.HSSP_OWNER_GROUP_NUMBER_DESC
                            , new.HSSP_PHYSICAL_PRESALES_DATE
                            , new.HSSP_PRESALES_DATE
                            , new.HSSP_PROJECTED_OPEN_DATE
                            , new.HSSP_REPORTING_REGION
                            , new.HSSP_SSS_CLASS_YEAR
                            , new.HSSP_SSS_COMP_INDICATOR
                            , new.HSSP_SSS_REGION
                            , new.HSSP_STRATEGY_REPORTING_REGION
                            , new.IS_ACTIVE
                            , new.IS_MOBILE_APP_USER
                            , new.LAST_CANCEL_DATE
                            , new.LAST_CHECKIN_DATE_AFTER_REGISTRATION
                            , new.LAST_JOIN_DATE
                            , new.LAST_JOIN_DATE_AFTER_HSSP
                            , new.LAST_PG_DAYPASS_REDEMPTION_DATE_AFTER_REGISTRATION
                            , new.LAST_USED_ON_DATE
                            , new.PARENT_TEEN_HAS_CHECKED_IN_FLAG
                            , new.PFX_USER_CREATION_DATE
                            , new.PFX_USER_ID
                            , new.PG_ADDRESS
                            , new.PG_ADDRESS_2
                            , new.PG_AMPERITY_ID
                            , new.PG_BIRTH_DATE
                            , new.PG_CITY
                            , new.PG_COUNTRY
                            , new.PG_CURRENT_MEMBER
                            , new.PG_CURRENT_MEMBER_CLASSIFICATION
                            , new.PG_EMAIL
                            , new.PG_EMAIL_OPT_IN
                            , new.PG_GENDER
                            , new.PG_MOBILE
                            , new.PG_NAME
                            , new.PG_NAME_LAST
                            , new.PG_SMS_OPT_IN
                            , new.PG_STATE
                            , new.PG_ZIP
                            , new.RECORD_TYPE
                            --, new.REPORT_CACHE_DATETIME
                            --, new.REPORT_CACHE_DATETIME_FORMATTED
                            --, new.REPORT_DATE
                            , new.TEEN_2019_REPEAT_FLAG
                            , new.TEEN_ADDRESS_1
                            , new.TEEN_ADDRESS_2
                            , new.TEEN_AGE_AT_REGISTRATION
                            , new.TEEN_AGREEMENT_ENTRY_SOURCE
                            , new.TEEN_AMPERITY_ID
                            , new.TEEN_BARCODE
                            , new.TEEN_BIRTH_DATE
                            , new.TEEN_CANCEL_DATE
                            , new.TEEN_CITY
                            , new.TEEN_CLUB_NUMBER
                            , new.TEEN_COUNTRY
                            , new.TEEN_DELETE_CCPA
                            , new.TEEN_DELETE_GDPR
                            , new.TEEN_DELETE_OTHER
                            , new.TEEN_EMAIL_ADDRESS
                            , new.TEEN_EMAIL_OPT_IN
                            , new.TEEN_FIRST_NAME
                            , new.TEEN_GENDER
                            , new.TEEN_GROUP_NAME
                            , new.TEEN_IS_ACTIVE
                            , new.TEEN_IS_BILLING
                            , new.TEEN_LAST_NAME
                            , new.TEEN_M_ID
                            , new.TEEN_MARKETING_OPT_IN
                            , new.TEEN_MEMBER_CLASSIFICATION_AT_SIGN_UP
                            , new.TEEN_MEMBER_NUMBER_UNIQUE
                            , new.TEEN_MEMBER_SID
                            , new.TEEN_MEMBER_TYPE
                            , new.TEEN_OPT_IN_CCPA
                            , new.TEEN_OPT_IN_GDPR
                            , new.TEEN_OPT_IN_OTHER
                            , new.TEEN_PARENT_FLAG
                            , new.TEEN_PARENT_SAME_HOUSEHOLD_FLAG
                            , new.TEEN_PF_JOIN_DATE
                            , new.TEEN_PRIMARY_PHONE
                            , new.TEEN_PROMOTION
                            , new.TEEN_PROSPECT_NUMBER_UNIQUE
                            , new.TEEN_PROSPECT_SID
                            , new.TEEN_REGISTRATION_DATE
                            , new.TEEN_SMS_OPT_IN
                            , new.TEEN_SOURCE
                            , new.TEEN_STATE
                            , new.TEEN_ZIP_CODE
                            , new.TOTAL_HSSP_VISIT_COUNT 
                        ) as DW_ROW_HASH
                    from 
                    {src_db}.{src_schema}.HSSP_MARKETING_JOURNEY new ) t1
                    left outer join  
                    (
                        select 
                            prev.*
                            from {src_db}.{src_schema}.HSSP_MARKETING_JOURNEY_PREV prev) t2
                    on t2.amperity_id = t1.amperity_id and t2.teen_amperity_id = t1.teen_amperity_id and t2.teen_club_number = t1.teen_club_number and t1.TEEN_PARENT_FLAG = t2.TEEN_PARENT_FLAG
                    where (t2.amperity_id IS NULL and t2.teen_amperity_id IS NULL and t2.teen_club_number IS NULL and t2.TEEN_PARENT_FLAG is NULL) or t1.DW_ROW_HASH <> t2.DW_ROW_HASH
                    )
                )
                file_format = (type = csv compression = none field_optionally_enclosed_by='"' null_if=''  )
                overwrite = true
                single = true
            header = true
            max_file_size=5368709120"""
            else:
                sql_command = f"""copy into @{src_db}.{src_schema}.{stage_name}/{s3_path}{file_name}
                        from (
                            select 
                                t1.*
                            from {src_db}.{src_schema}.HSSP_MARKETING_JOURNEY t1
                        )
                        file_format = (type = csv compression = none field_optionally_enclosed_by='"' null_if=''  )
                        overwrite = true
                        single = true
                    header = true
                    max_file_size=5368709120"""
            sql_params = []
            result = execute_commands(cursor, sql_command, sql_params)
            log.info("copy into performed")
            log.info(f"sql: {sql_command}")
            log.info(result)

            if result[0][0]==0:
                log.warning(f"Rows unloaded is {result[0][0]}")
                log.warning("Exiting sftp upload process")
                return
                
            # Upload
            log.info("Initializing s3 client utils")
            s3ClientUtil = S3ClientUtil.instance()
            
            log.info("Initializing sftp client utils")
            stfpClientUtil = SFTPClientUtil.instance()
        
           
            target_path = drop_location + file_name
            log.info(f"Starting upload of {target_path} to ftp location from s3 location:{s3_path+file_name}")
            
            sftp_session = stfpClientUtil.open_connection()
            
            log.info(f"Current files in the ftp location: {sftp_session.listdir(drop_location)}")
            with sftp_session.open(target_path, 'wb', 32768) as f:
               s3ClientUtil.s3_client.download_fileobj(s3_bucket, s3_path+file_name, f)
            
            cursor = connect_to_snowflake().cursor()
            
            sql_command = f"""create or replace table {src_db}.{src_schema}.HSSP_MARKETING_JOURNEY_PREV 
                                as (
                                    select 
                                        hash(t1.AMPERITY_ID, t1.TEEN_AMPERITY_ID, t1.TEEN_CLUB_NUMBER, t1.TEEN_PARENT_FLAG) as HSSP_MARKETING_JOURNEY_SID
                                        , t1.*
                                        , hash(
                                            t1.AMPERITY_ID
                                            , t1.CURRENT_MEMBER_TYPE
                                            , t1.CURRENT_STATUS_CODE
                                            , t1.EMAIL_OPT_IN
                                            , t1.FIRST_CHECKIN_DATE_AFTER_REGISTRATION
                                            , t1.FIRST_JOIN_DATE_AFTER_HSSP
                                            , t1.FIRST_USED_ON_DATE
                                            , t1.HS_CITY
                                            , t1.HS_NAME
                                            , t1.HS_STATE
                                            , t1.HSSP_ADA_NAME
                                            , t1.HSSP_ADA_STATUS
                                            , t1.HSSP_CLASS_YEAR
                                            , t1.HSSP_CLUB_CLOSE_DATE
                                            , t1.HSSP_CLUB_COUNTRY
                                            , t1.HSSP_CLUB_DESC
                                            , t1.HSSP_CLUB_NUMBER_PF
                                            , t1.HSSP_CLUB_NUMBER_PF_AND_DESC
                                            , t1.HSSP_CLUB_NUMBER_POS
                                            , t1.HSSP_CLUB_NUMBER_POS_AND_DESC
                                            , t1.HSSP_CLUB_OPEN_DATE
                                            , t1.HSSP_CLUB_STATE
                                            , t1.HSSP_CLUB_STATUS
                                            , t1.HSSP_CORPORATE_CLUB_FLAG
                                            , t1.HSSP_CORPORATE_REGION
                                            , t1.HSSP_DMA
                                            , t1.HSSP_FBC_NAME
                                            , t1.HSSP_HIGH_LEVEL_REGION
                                            , t1.HSSP_MARKETING_REGION
                                            , t1.HSSP_OPERATIONS_DISTRICT
                                            , t1.HSSP_OPS_REGION
                                            , t1.HSSP_OWNER_GROUP_NUMBER
                                            , t1.HSSP_OWNER_GROUP_NUMBER_DESC
                                            , t1.HSSP_PHYSICAL_PRESALES_DATE
                                            , t1.HSSP_PRESALES_DATE
                                            , t1.HSSP_PROJECTED_OPEN_DATE
                                            , t1.HSSP_REPORTING_REGION
                                            , t1.HSSP_SSS_CLASS_YEAR
                                            , t1.HSSP_SSS_COMP_INDICATOR
                                            , t1.HSSP_SSS_REGION
                                            , t1.HSSP_STRATEGY_REPORTING_REGION
                                            , t1.IS_ACTIVE
                                            , t1.IS_MOBILE_APP_USER
                                            , t1.LAST_CANCEL_DATE
                                            , t1.LAST_CHECKIN_DATE_AFTER_REGISTRATION
                                            , t1.LAST_JOIN_DATE
                                            , t1.LAST_JOIN_DATE_AFTER_HSSP
                                            , t1.LAST_PG_DAYPASS_REDEMPTION_DATE_AFTER_REGISTRATION
                                            , t1.LAST_USED_ON_DATE
                                            , t1.PARENT_TEEN_HAS_CHECKED_IN_FLAG
                                            , t1.PFX_USER_CREATION_DATE
                                            , t1.PFX_USER_ID
                                            , t1.PG_ADDRESS
                                            , t1.PG_ADDRESS_2
                                            , t1.PG_AMPERITY_ID
                                            , t1.PG_BIRTH_DATE
                                            , t1.PG_CITY
                                            , t1.PG_COUNTRY
                                            , t1.PG_CURRENT_MEMBER
                                            , t1.PG_CURRENT_MEMBER_CLASSIFICATION
                                            , t1.PG_EMAIL
                                            , t1.PG_EMAIL_OPT_IN
                                            , t1.PG_GENDER
                                            , t1.PG_MOBILE
                                            , t1.PG_NAME
                                            , t1.PG_NAME_LAST
                                            , t1.PG_SMS_OPT_IN
                                            , t1.PG_STATE
                                            , t1.PG_ZIP
                                            , t1.RECORD_TYPE
                                            --, t1.REPORT_CACHE_DATETIME
                                            --, t1.REPORT_CACHE_DATETIME_FORMATTED
                                            --, t1.REPORT_DATE
                                            , t1.TEEN_2019_REPEAT_FLAG
                                            , t1.TEEN_ADDRESS_1
                                            , t1.TEEN_ADDRESS_2
                                            , t1.TEEN_AGE_AT_REGISTRATION
                                            , t1.TEEN_AGREEMENT_ENTRY_SOURCE
                                            , t1.TEEN_AMPERITY_ID
                                            , t1.TEEN_BARCODE
                                            , t1.TEEN_BIRTH_DATE
                                            , t1.TEEN_CANCEL_DATE
                                            , t1.TEEN_CITY
                                            , t1.TEEN_CLUB_NUMBER
                                            , t1.TEEN_COUNTRY
                                            , t1.TEEN_DELETE_CCPA
                                            , t1.TEEN_DELETE_GDPR
                                            , t1.TEEN_DELETE_OTHER
                                            , t1.TEEN_EMAIL_ADDRESS
                                            , t1.TEEN_EMAIL_OPT_IN
                                            , t1.TEEN_FIRST_NAME
                                            , t1.TEEN_GENDER
                                            , t1.TEEN_GROUP_NAME
                                            , t1.TEEN_IS_ACTIVE
                                            , t1.TEEN_IS_BILLING
                                            , t1.TEEN_LAST_NAME
                                            , t1.TEEN_M_ID
                                            , t1.TEEN_MARKETING_OPT_IN
                                            , t1.TEEN_MEMBER_CLASSIFICATION_AT_SIGN_UP
                                            , t1.TEEN_MEMBER_NUMBER_UNIQUE
                                            , t1.TEEN_MEMBER_SID
                                            , t1.TEEN_MEMBER_TYPE
                                            , t1.TEEN_OPT_IN_CCPA
                                            , t1.TEEN_OPT_IN_GDPR
                                            , t1.TEEN_OPT_IN_OTHER
                                            , t1.TEEN_PARENT_FLAG
                                            , t1.TEEN_PARENT_SAME_HOUSEHOLD_FLAG
                                            , t1.TEEN_PF_JOIN_DATE
                                            , t1.TEEN_PRIMARY_PHONE
                                            , t1.TEEN_PROMOTION
                                            , t1.TEEN_PROSPECT_NUMBER_UNIQUE
                                            , t1.TEEN_PROSPECT_SID
                                            , t1.TEEN_REGISTRATION_DATE
                                            , t1.TEEN_SMS_OPT_IN
                                            , t1.TEEN_SOURCE
                                            , t1.TEEN_STATE
                                            , t1.TEEN_ZIP_CODE
                                            , t1.TOTAL_HSSP_VISIT_COUNT 
                                        ) as DW_ROW_HASH
                                    from {src_db}.{src_schema}.HSSP_MARKETING_JOURNEY t1)"""
            sql_params = []
            result = execute_commands(cursor, sql_command, sql_params)
            log.info(f"clone performed: {sql_command}")
            log.info(result)
        except Exception as ex:
            log.error(f"Exception  in fetch_hssp_marketing_journey: {ex}")
            log.error(type(ex).__name__, __file__, ex.__traceback__.tb_lineno)
            raise ex
        finally:
            # Close
            if sftp_session:
                stfpClientUtil.close_connection(sftp_session)
            if cursor:
                cursor.close()
        # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
        # cursor.execute(sql_command)
        # df = cursor.fetch_pandas_all()
        
        # memStats        = df.memory_usage()

        # log.info("Memory consumption of each DataFrame column in bytes:")
        # log.info(memStats);
        # log.info("Memory consumption of the DataFrame instance in bytes:%d bytes"%(memStats.sum()))
        # log.info("Memory consumption in megabytes(MB): %2.2f MB"%(memStats/1024/1024).sum())
        # ssh_client = paramiko.SSHClient()
        # ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh_client.connect(hostname='host',username='user_name',password='password')
        # ftp_client= ssh_client.open_sftp()
        # with ftp_client.open('/path/on/ftp/server/file.csv', 'w', bufsize=32768) as f:
        #     f.write(df.to_csv(index=False))
        # df = None