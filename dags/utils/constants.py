from utils.parse_variable import get_variable

SPENDS_AGENCIES = {'TEAMLIFT': 'Team Lift','MOROCH': 'Moroch','ZIMMERMAN': 'Zimmerman Advertising LLC', 'CROSSMEDIA': 'Cross Media'}

def get_email_list(ops_environment):
    email_list_nonprod = ['PfDataNotificationsNonProd@pfhq.com']
    email_list_prod = ['PfDataNotificationsProd@pfhq.com']

    if ops_environment:
            ops_environment = ops_environment.lower()
            if ops_environment == 'dev_ops' or ops_environment == 'uat_ops':
                return email_list_nonprod 
            elif ops_environment == 'prod_ops' :
                return email_list_prod
    
def get_excluded_tbl_names():
    excluded_table_list = [ "EXT_GROSS_RATING_POINT,EXT_GROSS_RATING_POINT_FORECAST","EXT_NATIONAL_TV_SPENDS_DAY_PART_ACTUAL"
    ,"EXT_NAF_TV_SPENDS_PLANNED"
    ,"EXT_NAF_TV_SPENDS_ACTUAL"
    ,"EXT_LOCAL_SPENDS_PLANNED_PRORATED_DAILY"
    ,"EXT_DIGITAL_SPENDS_PLANNED_MONTHLY"
    ,"EXT_IFIT_AUDIENCE_ERROR"
    ,"EXT_DIGITAL_SPENDS_PLANNED"
    ,"EXT_LOCAL_SPENDS_ACTUAL_PRORATED_DAILY"
    ,"EXT_IFIT_EQUIPMENT_AUDIENCE"
    ,"EXT_NAF_HISTORICAL_SPENDS_HILL_HOLIDAY"
    ,"EXT_AMAZON_SPENDS_ACTUAL"
    ,"EXT_SESSION"
    ,"EXT_SALES_PERIOD"
    ,"EXT_JOINS_FORECAST"
    ,"EXT_SALES_PERIOD_DAILY"]
    return excluded_table_list

def get_agency_email_list(agency):
    if agency == SPENDS_AGENCIES['TEAMLIFT']:
        return ['teamliftnotifications@pfhq.com']
    elif agency == SPENDS_AGENCIES['MOROCH']:
        return ['morochnotifications@pfhq.com' ]
    elif agency == SPENDS_AGENCIES['ZIMMERMAN']:
        return ['zimmermannotifications@pfhq.com']
    elif agency == SPENDS_AGENCIES['CROSSMEDIA']:
        return ['crossmedianotifications@pfhq.com']  
    else:
        return []


def get_domain_list(agency):
    if agency == SPENDS_AGENCIES['TEAMLIFT']:
        return ['local_planned', 'local_placed', 'national_placed']
    else:
        return ['local_planned', 'local_placed']

def get_sm_secret_id(domain):
    SM_SFMC_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/sftp_sfmc_sm'.format(get_variable("env"))
    SM_APT_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/sftp_apt_spends_sm'.format(get_variable("env"))
    SM_NAF_SPENDS_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/aws_secret_naf_crossmedia'.format(get_variable("env"))
    SM_TANGO_SECRET_ID_NAME = 'pf-aas-airflow-{}/connections/sftp_tango'.format(get_variable("env"))

    if domain.lower() == "sfmc":
        return  SM_SFMC_SECRET_ID_NAME 
    elif domain.lower() == "apt-spends":
        return SM_APT_SECRET_ID_NAME
    elif domain.lower() == "naf-spends":
        return SM_NAF_SPENDS_SECRET_ID_NAME
    elif domain.lower() == "tango":
        return SM_TANGO_SECRET_ID_NAME
    else:
        return ""

