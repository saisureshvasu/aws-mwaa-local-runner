import logging
import sys
import io
import csv
import pandas as pd
import inspect
from utils.email_util import send_custom_email
from utils.s3_client_util import S3ClientUtil
from utils.sf_client_util import SFClientUtil

log = logging.getLogger(__name__)

class LeadGenUtil:
        
    @staticmethod
    def get_leads_data(processing_date, record_type_id):
        """
        Utility function that retrieves Lead data using soql queries for form based entries
        Note: processing_date is not a mandatory field, it can be used to pull up specific date data

        Returns:
            dataframe: leads data in data frame format
        """
        log.info(inspect.getargvalues(inspect.currentframe()))
        if processing_date is not None:
            query_soql_lead = """ 
                SELECT 
                    Email
                    , FirstName
                    , LastName
                    ,Campaign_Name__c
                    , Campaign_Date__c
                    , POS_Club_ID__c 
                    , Group_Number__c
                    , Canada_Email_Opt_In__c
                FROM Lead
                WHERE RecordTypeId = '{0}'
                AND DAY_ONLY(Campaign_Date__c)>={1}""".format(record_type_id, processing_date) 
        else: 
            query_soql_lead = """ 
                SELECT 
                    Email
                    , FirstName
                    , LastName
                    ,Campaign_Name__c
                    , Campaign_Date__c
                    , POS_Club_ID__c 
                    , Group_Number__c
                    , Canada_Email_Opt_In__c
                FROM Lead
                WHERE RecordTypeId = '{0}'""".format(record_type_id)
        sfClientUtil = SFClientUtil.instance()
        log.info(query_soql_lead)
        
        log.info("querying query_soql_lead")
        df_lead_records = sfClientUtil.sf_query(query_soql_lead)
        log.info("number of records: {}".format(df_lead_records.size))
        return df_lead_records

    @staticmethod
    def get_file_list_data(last_modified_date, file_prefix):
        """
        Utility function that retrieves Lead data using soql queries for file based entries in salesforce

        Args:
            last_modified_date (string)
            file_prefix (string)

        Returns (list): list of files and it's metadata in data frame format
        
        """
        log.info(inspect.getargvalues(inspect.currentframe()))
        try:
            query_soql_cv = """ 
                SELECT 
                    VersionData 
                    , ContentDocumentId
                    , FileExtension
                    , Submitter_Email__c
                FROM ContentVersion
                WHERE ContentDocumentId in (SELECT ID FROM ContentDocument WHERE Title LIKE '{0}')
                AND IsLatest = true""".format(file_prefix)
            
            if last_modified_date is not None:
                query_soql_cd = """ 
                    SELECT 
                        ID
                        , Title 
                        , CreatedDate 
                    FROM ContentDocument
                    WHERE Title LIKE '{0}'
                    AND DAY_ONLY(CreatedDate) >= {1}""".format(file_prefix, last_modified_date)
            else:
                query_soql_cd = """ 
                    SELECT 
                        ID
                        , Title 
                        , CreatedDate 
                    FROM ContentDocument
                    WHERE Title LIKE '{0}'""".format(file_prefix)
            log.info(query_soql_cd)
            log.info(query_soql_cv)
            
            sfClientUtil = SFClientUtil.instance()
            
            log.info("querying query_soql_cv")
            df_records_cv = sfClientUtil.sf_query(query_soql_cv)
            
            log.info("querying query_soql_cd")
            df_records_cd = sfClientUtil.sf_query(query_soql_cd)
            
            log.info("number of records: {}".format(df_records_cd.size))

            if df_records_cd is None or df_records_cd.empty:
                log.info("No records found")
                return pd.DataFrame()
            else:
                return df_records_cd.merge(df_records_cv, how='inner', left_on='Id', right_on='ContentDocumentId')
        except Exception as ex:
            print(ex)


    @staticmethod
    def get_file_data(file_url):
        """
        Utility function that retrieves a file content

        Args:
            file_url (string)

        Returns:
            (list) : list of files and it's metadata in data frame format
        """
        log.info(inspect.getargvalues(inspect.currentframe()))
        sfClientUtil = SFClientUtil.instance()
        return sfClientUtil.sf_client.session.get('https://{0}{1}'.format(sfClientUtil.sf_client.sf_instance, file_url), headers=sfClientUtil.sf_client.headers)

    @staticmethod
    def validate_and_transform_leads_data(df_presales_leads_raw, email_dl, email_dryrun=False, email_environment=None):
        """
        function to validate leads data
        note: email is sent to the usual airflow configured email list
        Args:
            df_presales_leads_raw (dataframe): leadgen raw data 
            email_dl (list): email distribution list
            email_dryrun (bool, optional):  Defaults to False.
            email_environment (string, optional): . Defaults to None.

        Returns:
            Boolean: True if valid and False if invalid data
        """     
        log.info("enter:::validate_and_transform_leads_data")  
        log.info(inspect.getargvalues(inspect.currentframe()))
        if not df_presales_leads_raw.empty:
            df_presales_leads = df_presales_leads_raw.rename(
                columns={"FirstName": "First Name", "LastName": "Last Name",
                            "POS_Club_ID__c": "Club Number",
                            "Group_Number__c": "Group Number",
                            "Campaign_Name__c": "Campaign Name",
                            "Campaign_Date__c": "Campaign Date",
                            "Canada_Email_Opt_In__c": "Email Opt-in"}). \
                drop(columns=['attributes']).dropna(how='all')[["Email", "First Name", "Last Name", "Campaign Name",
                                                                "Campaign Date", "Club Number", "Group Number",
                                                                "Email Opt-in"]]

            df_presales_leads['Birthdate'] = None
            df_presales_leads['Phone'] = None
            df_presales_leads['Address Line 1'] = None
            df_presales_leads['Address Line 2'] = None
            df_presales_leads['City'] = None
            df_presales_leads['State'] = None
            df_presales_leads['Zip'] = None
            df_presales_leads['Record'] = df_presales_leads.index + 1
            df_presales_leads['Validation'] = None
            df_presales_leads.loc[df_presales_leads['Email Opt-in'].isnull(), 'Email Opt-in'] = 'FALSE'

            # Date validations
            df_presales_leads['Campaign Date'] = pd.to_datetime(df_presales_leads['Campaign Date'], format='%Y-%m-%d',
                                                                errors='coerce').dt.date
            df_presales_leads.loc[
                df_presales_leads['Campaign Date'].isnull(), 'Validation'] = 'CAMPAIGN DATE NOT IN FORMAT YYYY-MM-DD'

            # Email Validation
            df_presales_leads.loc[(df_presales_leads['Email'].str.match(
                r'''([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)''') == False & df_presales_leads[
                                        'Email'].notnull()), 'Validation'] = 'BAD EMAIL ADDRESS FORMAT'
            # Club Number Validation
            df_presales_leads.loc[(df_presales_leads['Club Number'].str.match(
                r'''^[0-9]*$''') == False & df_presales_leads[
                                        'Club Number'].notnull()), 'Validation'] = 'NON-NUMERIC CLUB NUMBER'
            df_presales_leads.loc[df_presales_leads['Club Number'].str.len() > 6, 'Validation'] = 'ClUB NUMBER TOO LONG'

            # Group Number Validation
            df_presales_leads.loc[
                df_presales_leads['Group Number'].str.len() > 6, 'Validation'] = 'GROUP NUMBER TOO LONG'

            # Missing mandatory columns validation
            df_presales_leads.loc[(df_presales_leads['Email'].isnull()) |
                                    (df_presales_leads['First Name'].isnull()) |
                                    (df_presales_leads['Last Name'].isnull()) |
                                    (df_presales_leads['Campaign Name'].isnull()) |
                                    (df_presales_leads['Campaign Date'].isnull()) |
                                    (df_presales_leads['Club Number'].isnull()) |
                                    (df_presales_leads[
                                        'Group Number'].isnull()), 'Validation'] = 'MISSING MANDATORY VALUE'
            
            df_mandatory_miss_cols = df_presales_leads[(df_presales_leads['Email'].isnull()) |
                                                        (df_presales_leads['First Name'].isnull()) |
                                                        (df_presales_leads['Last Name'].isnull()) |
                                                        (df_presales_leads['Campaign Name'].isnull()) |
                                                        (df_presales_leads['Campaign Date'].isnull()) |
                                                        (df_presales_leads['Club Number'].isnull()) |
                                                        (df_presales_leads['Group Number'].isnull()) |
                                                        (df_presales_leads['Validation'].notnull())]
            log.info("The size of df_mandatory_miss_cols")                                           
            
                                                         
            if not df_mandatory_miss_cols.empty:
                feedback = "Presales records have been fetched for processing, but encountered following " \
                            "errors:\n \n"
                for bad_row in df_mandatory_miss_cols.iterrows():
                    feedback = feedback + """ - Record # {} Rejected with message: '{}'\n""".format(
                        bad_row[1]['Record'],
                        bad_row[1]['Validation'])

                feedback = feedback + "\nPresales records will partially processed - the errant records listed above " \
                                        "will not be added to Marketing Cloud.\n \n"
                log.info(feedback)
                log.info("Sending feedback email\n")
                send_custom_email(to_email=email_dl, subject="[{}] Presales Leads Processing Feedback".format(email_environment),
                    html_content="""<p style="font-size:15px">""" + feedback.replace('\n', '<br/>') + """</p>""", email_dryrun=email_dryrun)
            df_presales_leads = df_presales_leads[(df_presales_leads['Email'].notnull()) &
                                                    (df_presales_leads['First Name'].notnull()) &
                                                    (df_presales_leads['Last Name'].notnull()) &
                                                    (df_presales_leads['Campaign Name'].notnull()) &
                                                    (df_presales_leads['Campaign Date'].notnull()) &
                                                    (df_presales_leads['Club Number'].notnull()) &
                                                    (df_presales_leads['Group Number'].notnull()) &
                                                    ~(df_presales_leads['Validation'].notnull())]

            if not df_presales_leads.empty:
                log.info(df_presales_leads.head(1))
                feedback = "All valid presales leads records have been processed successfully! \n"
                log.info(f'Feedback: {feedback}')
                return df_presales_leads, True
            else:
                feedback = "All fetched presales leads records have been rejected! \n"
                log.info(f'Feedback: {feedback}')
                return None, False
        else:
            feedback = "No Presales lead gen records found in Salesforce today for processing"
            log.info(f'Feedback: {feedback}')
            return None, False


    @staticmethod
    def validate_file_data(bucket, email_dryrun, file_config, source_path, file_name):
        """
        function to validate file data
        note: email is sent to the file uploader in salesforce
        Args:
            bucket (string): S3 bucket name
            email_dryrun (boolean): 
            file_config (dict): 
            source_path (string): source file path
            file_name (string): file_name eg: YYYYMMDD/filename.extension
        """   
        log.info("enter:::validate_file_data")  
        log.info(inspect.getargvalues(inspect.currentframe())) 
        log.info(file_config)    
        file_title = file_config[1]['Title']
        file_extension = file_config[1]['FileExtension']
        log.info("file_title"+file_title) 
        log.info("file_extension"+file_extension) 
        log.info("validate_file_data before try block")  
        try:
            s3ClientUtil = S3ClientUtil.instance()
            data = s3ClientUtil.read_object_in_bytes(bucket,
                                                    '{}/{}'.format(source_path, file_name))
            log.info("validate_file_data completed reading the data from file")                                    
            df_excel = pd.read_excel(io.BytesIO(data),
                                        usecols=['Email', 'First Name', 'Last Name',
                                                'Campaign Name', 'Campaign Date',
                                                'Club Number', 'Group Number',
                                                'Birthdate', 'Phone', 'Address Line 1',
                                                'Address Line 2', 'City', 'State', 'Zip',
                                                'Record', 'Validation'], convert_float=False,
                                        converters={'Club Number': str, 'Phone': str, 'Zip': str}).dropna(how='all')
            log.info("df_excel size")
            
            if df_excel.empty:
                feedback = "File {} is empty hence this file will be rejected, Please upload new file if required.\n ". \
                    format(file_name)
                log.info(feedback)
                log.info("Sending feedback email\n")
                send_custom_email(to_email=file_config[1]['Submitter_Email__c'], subject="Leadgen File Processing Feedback",
                    html_content="""<p style="font-size:15px">""" + feedback.replace('\n', '<br/>') + """</p>""", email_dryrun=email_dryrun)

            else:
                # Column Addition
                df_excel['Email Opt-in'] = None

                # Format conversion
                df_excel['Record'] = df_excel['Record'].astype(int)
                df_excel['Club Number'] = df_excel['Club Number'].replace(r'\.0$', "", regex=True)
                df_excel['Phone'] = df_excel['Phone'].replace(r'\.0$', "", regex=True)
                df_excel['Zip'] = df_excel['Zip'].replace(r'\.0$', "", regex=True)

                # Date Validations
                df_excel['Campaign Date'] = pd.to_datetime(df_excel['Campaign Date'], format='%Y-%m-%d',
                                                            errors='coerce')
                df_excel['Birthdate'] = pd.to_datetime(df_excel['Birthdate'], format='%Y-%m-%d', errors='coerce')
                df_excel.loc[
                    df_excel['Campaign Date'].isnull(), 'Validation'] = 'CAMPAIGN DATE NOT IN FORMAT YYYY-MM-DD'

                # Email Validation
                df_excel.loc[(df_excel['Email'].str.match(
                    r'''([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)''') == False & df_excel[
                                    'Email'].notnull()), 'Validation'] = 'BAD EMAIL ADDRESS FORMAT'
                # Club Number Validation
                df_excel.loc[(df_excel['Club Number'].str.match(
                    r'''^[0-9]*$''') == False & df_excel[
                                    'Club Number'].notnull()), 'Validation'] = 'NON-NUMERIC CLUB NUMBER'
                
                # Missing mandatory columns validation
                df_mandatory_miss_cols = df_excel[(df_excel['Email'].isnull()) |
                                                    (df_excel['First Name'].isnull()) |
                                                    (df_excel['Last Name'].isnull()) |
                                                    (df_excel['Campaign Name'].isnull()) |
                                                    (df_excel['Campaign Date'].isnull()) |
                                                    (df_excel['Club Number'].isnull()) |
                                                    (df_excel['Group Number'].isnull()) |
                                                    (df_excel['Validation'].notnull())]
                log.info("df_mandatory_miss_cols")
                
                if not df_mandatory_miss_cols.empty:
                    feedback = "Your file {} has been uploaded for processing, but encountered the following " \
                                "errors:\n \n".format(file_title + '.' + file_extension)
                    for bad_row in df_mandatory_miss_cols.iterrows():
                        feedback = feedback + """ - Record # {} Rejected with message: '{}'\n""".format(
                            bad_row[1]['Record'],
                            bad_row[1]['Validation'])
                    feedback = feedback + "\nYour file will only be partially processed - the errant records listed above " \
                                            "will not be added to your Marketing Cloud account.\n \n"
                    feedback = feedback + "Please correct the records listed above and upload these records in a separate " \
                                            "file.\n "
                else:
                    feedback = "Your file {} has been uploaded successfully! Your data will be available in your " \
                                "Marketing Cloud account in 1-2 days. \n".format(file_title + '.' + file_extension)

                df_excel = df_excel[(df_excel['Email'].notnull()) &
                                    (df_excel['First Name'].notnull()) &
                                    (df_excel['Last Name'].notnull()) &
                                    (df_excel['Campaign Name'].notnull()) &
                                    (df_excel['Campaign Date'].notnull()) &
                                    (df_excel['Club Number'].notnull()) &
                                    (df_excel['Group Number'].notnull()) &
                                    ~(df_excel['Validation'].notnull())]

                # To rearrange columns before writing
                df_excel = df_excel[
                    ["Email", "First Name", "Last Name", "Campaign Name", "Campaign Date", "Club Number",
                        "Group Number", "Email Opt-in", "Birthdate", "Phone", "Address Line 1", "Address Line 2", "City",
                        "State", "Zip", "Record", "Validation"]]

                log.info(f'Feedback: {feedback}')
                log.info(type(df_excel))
                log.info(df_excel)
                log.info(type(df_mandatory_miss_cols))
                log.info(df_mandatory_miss_cols)
                log.info("Sending feedback email\n")
                send_custom_email(to_email=file_config[1]['Submitter_Email__c'], subject="Leadgen File Processing Feedback",
                    html_content="""<p style="font-size:15px">""" + feedback.replace('\n', '<br/>') + """</p>""", email_dryrun=email_dryrun)
                
                return df_excel, len(df_mandatory_miss_cols)==0

        except ValueError as ve:
            feedback = "Incorrect template used for file {} hence this file will be rejected, please use correct " \
                        "template from FRM Portal and upload new file again.\n ".format(file_title + '.' + file_extension)
            feedback = feedback + "\nFollowing message may describe the exact reason for the failure:" + \
                        str(sys.exc_info()[1]) + "\n"
            log.info(f'Feedback: {feedback}')
            log.info("Sending feedback email\n")
            send_custom_email(to_email=file_config[1]['Submitter_Email__c'], subject="Leadgen File Processing Feedback",
                html_content="""<p style="font-size:15px">""" + feedback.replace('\n', '<br/>') + """</p>""", email_dryrun=email_dryrun)
        return None, False