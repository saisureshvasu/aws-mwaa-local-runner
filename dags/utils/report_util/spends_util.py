from utils.constants import get_agency_email_list, get_domain_list, SPENDS_AGENCIES
import logging
from datetime import datetime
from airflow.utils.email import send_email
from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import pandas as pd
log = logging.getLogger(__name__)

def spends_late_data_alert(params):
    """spends_late_data_alert: function to send the spends late data alert to respective agencies

    Args:
        params (dict): dictionary object with paramter details
    """
    try:
        log.info(f'Executing spends_late_data_alert')
        cursor = connect_to_snowflake().cursor()
        sql_params = None
        database = params["gold_environment"]

        for agency in SPENDS_AGENCIES.values():

            # Spends summary report SQL
            spends_data_arrival_sql = f"""
            select 
                sum(ROW_COUNT) ROW_COUNT, DOMAIN
                from 
                    {database}.CDW.VW_SPENDS_LOADING_HISTORY
                where                     
                    INGESTION_PROCESS = 'api-process' and
                    INGESTION_DATE >= dateadd(day,-6,current_date) and 
                    agency = '{agency}'
                group by
                    DOMAIN"""
            log.info(f'SQL for Spends Data Arrival: {spends_data_arrival_sql}')

            # Executing the SQL for spends data arrival            
            spends_data_arrival_result = execute_commands(cursor, spends_data_arrival_sql, sql_params)

            # Keeping domain names only if the row count is greater than 0
            spends_data_arrival_result = [result_set[1] for result_set in spends_data_arrival_result if result_set[0] > 0]
            log.info(f'spends_data_arrival_result: {spends_data_arrival_result}')

            # Check whether data arrivied as per schedule for all domains 
            incomplete_spends_domain = []
            max_date_str = ''
            spends_domain_list = get_domain_list(agency)
            for domain in spends_domain_list:
                if domain not in spends_data_arrival_result:
                    incomplete_spends_domain.append(domain)
                    spends_max_data_sql = f"""
                    select 
                        max(date) MAX_DATE
                        from 
                            {database}.CDW.VW_SPENDS_LOADING_HISTORY
                        where                     
                            INGESTION_PROCESS = 'api-process' and
                            AGENCY = '{agency}' and
                            DOMAIN = '{domain}'"""                                                 
                    spends_max_data_sql_result = execute_commands(cursor, spends_max_data_sql, sql_params)
                    log.info(f'spends_max_data_sql_result: {spends_max_data_sql_result}')
                    if spends_max_data_sql_result[0][0] is None:
                        max_date_str += f'No records for {domain}.<br>'
                    else:
                        max_date_str += f'The latest record for {domain} is {spends_max_data_sql_result[0][0].strftime("%m-%d-%Y")}.<br>' 
                    log.info(f'max_date_str: {max_date_str}')                        

            today_date = datetime.now().strftime("%m-%d-%Y")
            log.info(f'incomplete spends domain: {incomplete_spends_domain}, length: {len(incomplete_spends_domain)}')

            # If there are incomplete domains then send the email data alert
            if len(incomplete_spends_domain) > 0:
                
                email_list = get_agency_email_list(agency)
                log.info("Function spends_late_data_alert: Sending report email to", email_list)

                # Send email to respective agencies
                incomplete_spends_domain = ', '.join(map(str, incomplete_spends_domain)).replace('_', ' ')
                max_date_str = max_date_str.replace('_', ' ')
                send_email(
                    to=email_list,
                    subject=f'{agency} Late Data Alert {today_date}',
                    html_content=f'Hi Team,<br><br>We have incomplete spends data for {incomplete_spends_domain}.<br>{max_date_str}<br><br>Thanks'
                )

    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in spends_late_data_alert function")

def send_spends_summary_report(params):
    """send_spends_summary_report: function to send the spends summary report to respective agencies

    Args:
        params (dict): dictionary object with paramter details
    """
    try:
        log.info(f'Executing send_spends_summary_report')
        cursor = connect_to_snowflake().cursor()
        sql_params = None
        database = params["gold_environment"]

        for agency in SPENDS_AGENCIES.values():
            # Define Excel properties
            today_date = datetime.now().strftime("%m-%d-%Y")
            writer = pd.ExcelWriter(f"/tmp/{agency} Summary Report "+today_date+".xlsx", engine='xlsxwriter')
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#1E90FF',
                'font_color': '#FFFFFF',
                'border': 1})
            data_available_flag = False
            total_spends_columns = ['DATE', 'CHANNEL', 'DMA', 'TOTAL_SPENDS']
            spends_tables = {"placed": "FACT_LOCAL_SPENDS_PLACED", "planned": "FACT_LOCAL_SPENDS_PLANNED"}
            for domain in spends_tables.keys():
                # Total Spends SQL
                total_spends_sql = f"""
                select
                    DATE,
                    CHANNEL,
                    DMA,
                    sum(SPEND) TOTAL_SPEND
                from
                    {database}.CDW.{spends_tables[domain]}
                where
                    DATE >= DATEADD(MONTH, -1, CURRENT_DATE)
                group by
                    CHANNEL,
                    DMA,
                    DATE
                order by
                    CHANNEL,
                    DMA,
                    DATE,
                    TOTAL_SPEND            
                """
                
                total_spends_result = execute_commands(cursor, total_spends_sql, sql_params)
                log.info(f'total spends placed result: {total_spends_result}')
                total_spends_dataframe = pd.DataFrame(
                    total_spends_result, columns=total_spends_columns) 
                total_spends_dataframe.set_index("DATE", inplace=True)                
                log.info(f'Total Spends {domain}: {total_spends_dataframe}, {total_spends_dataframe.size}')            
                            
                # Excel Sheet for Summary Report: Write the Summary Report sheet only when there are results
                if total_spends_dataframe.size > 0:
                    data_available_flag = True
                    total_spends_dataframe.to_excel(
                        excel_writer=writer, sheet_name=domain, startrow=1, header=False)
                    worksheet = writer.sheets[domain]
                    # Write the column headers with the defined format
                    for col_num, value in enumerate(total_spends_columns):
                        worksheet.write(0, col_num, value, header_format)
                    
            writer.save()

            email_list = get_agency_email_list(agency)
            log.info("Function send_spends_summary_report: Sending report email to", email_list)

            # Send email to respective agencies
            if data_available_flag: 
                send_email(
                    to=email_list,
                    subject=f'{agency} Summary Report {today_date}',
                    html_content=f'Hi Team,<br><br>Please find the Summary Report for {today_date}.<br><br>Thanks',
                    files=[f'/tmp/{agency} Summary Report '+today_date+'.xlsx']
                )
            else:
                send_email(
                    to=email_list,
                    subject=f'{agency} Summary Report {today_date}',
                    html_content=f'Hi Team,<br><br>Data unavailable for Summary Report for {today_date}.<br><br>Thanks'
                )
            



    except Exception as ex:
        log.error("--------------------Exception log start--------------------")
        log.error(f"Exception: {ex}")
        log.error(f"  Type: {type(ex).__name__}")
        log.error(f"  File path: {__file__}")
        log.error(f"  Line no: {ex.__traceback__.tb_lineno}")
        log.error("--------------------Exception log end--------------------")
        raise Exception(f"Exception in send_spends_summary_report function")