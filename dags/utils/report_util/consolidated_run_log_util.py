from utils.mdp.snowflake_utils import connect_to_snowflake, execute_commands
import logging
from datetime import  datetime
from airflow.utils.email import send_email
import pandas as pd
log = logging.getLogger(__name__)



def publish_run_log_report(params):
    try:
        cursor = connect_to_snowflake().cursor()
        sql_params = None

        goldTblSqlCommnd = """SELECT TGT_TBL, ifnull(RAW_TABLE_COUNT,0) RAW_TABLE_COUNT , GOLD_TABLE_COUNT,
             (GOLD_TABLE_COUNT - ifnull(RAW_TABLE_COUNT,0))::number AS DIFFERENCE from CNTRL.VW_TASK_RUN_LOG_FROM_RAW_TBL
            where cast(START_TIME as date) = current_date;"""

        goldCountRslt = execute_commands(cursor,goldTblSqlCommnd,sql_params)        
        goldCountColumns = ['TABLE_NAME','RAW_TABLE_COUNT','SNOWFLAKE_TABLE_COUNT','DIFFERENCE'] #dfCols
        goldCountDataFrame = pd.DataFrame(goldCountRslt,columns=goldCountColumns) #df
        #goldCountDataFrame.set_index("TABLE_NAME",inplace=True) #df

        runLogSqlCommnd = """select TGT_TBL, START_TIME, END_TIME, DURATION_IN_SEC, TASK_INSTANCE_ID,
        INSERT_ROW_COUNT+UPDATE_ROW_COUNT from CNTRL.VW_TASK_RUN_LOG_FROM_RAW_TBL
         where cast(START_TIME as date) = current_date;"""
         
        runLogRslt = execute_commands(cursor,runLogSqlCommnd,sql_params)        
        runLogResultColumns = ['TABLE_NAME', 'START_TIME', 'END_TIME', 'DURATION_IN_SEC', 'TASK_INSTANCE_ID', 'INSERT_ROW_COUNT+UPDATE_ROW_COUNT'] #df1Cols
        runLogResultDataFrame = pd.DataFrame(runLogRslt,columns=runLogResultColumns) #df1
        #runLogResultDataFrame.set_index("TABLE_NAME",inplace=True)                 

        dqEltRuntimeSqlCommnd = "select * from CNTRL.VW_DQ_ELT_RUNTIME_FROM_RAW_TABLE"
        dqEltRuntimeQryRslt = execute_commands(cursor,dqEltRuntimeSqlCommnd,sql_params)        
        dqEltRuntimeQryTablsColumns = ['TABLE_NAME','DQ_RUNTIME_IN_SECS','ELT_RUNTIME_IN_SECS','DQ_AND_ELT_RUNTIME_IN_SECS','START_TIME','END_TIME']
        dqEltRuntimeQryTablsDataFrame = pd.DataFrame(dqEltRuntimeQryRslt,columns=dqEltRuntimeQryTablsColumns)  
        #dqEltRuntimeQryTablsDataFrame.set_index("TABLE_NAME",inplace=True)

        # code added for current table: to check for last 5 days avg count difference with the same table 
        tableCountSqlCommnd = """select A.TGT_TBL,AVG(A.GOLD_TABLE_COUNT),max(B.GOLD_TABLE_COUNT),round((max(B.GOLD_TABLE_COUNT) - AVG(A.GOLD_TABLE_COUNT))) AS DIFF,

        round((  max(B.GOLD_TABLE_COUNT)-AVG(A.GOLD_TABLE_COUNT))*100 / AVG(A.GOLD_TABLE_COUNT),2) AS PERC_DIFF ,current_date() from CNTRL.VW_TASK_RUN_LOG A

        JOIN (select TGT_TBL,GOLD_TABLE_COUNT from CNTRL.VW_TASK_RUN_LOG where to_date(start_time)=current_date()) B

        ON A.TGT_TBL = B.TGT_TBL GROUP BY A.TGT_TBL"""        
        tableCountQryRslt = execute_commands(cursor,tableCountSqlCommnd,sql_params)        
        tableCountQryTablsColumns = ['TABLE_NAME','Average','Current_Table_Count','Difference','% Difference','Date']
        tableCountQryTablsDataFrame = pd.DataFrame(tableCountQryRslt,columns=tableCountQryTablsColumns)  
        #tableCountQryTablsDataFrame.set_index("TABLE_NAME",inplace=True)
        # code end for count         
         
        now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        today_date = datetime.now().strftime("%m-%d-%Y")
        writer = pd.ExcelWriter("/tmp/run_snowflake_report_"+now+".xlsx",engine='xlsxwriter')

        goldCountDataFrame.to_excel(excel_writer = writer,sheet_name='count',startrow=1, header=False,index=False) #df

        workbook  = writer.book
        worksheet = writer.sheets['count'] 
        
        
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#1E90FF',
            'font_color':'#FFFFFF',
            'border': 1})
        cell_format = workbook.add_format({'align': 'left'})
        # Write the column headers with the defined format.

        for col_num, value in enumerate(goldCountColumns): #dfCols
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, 1, 18, cell_format)

        runLogResultDataFrame.to_excel(writer,sheet_name='run_log',startrow=1, header=False,index=False) #df1 
        worksheet = writer.sheets['run_log'] 
        # Write the column headers with the defined format.
        for col_num, value in enumerate(runLogResultColumns): #df1Cols
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, 1, 18, cell_format)
           
        #excel-sheet for Dq_elt_runtime: Write the DQ sheet only when there are results
        if dqEltRuntimeQryTablsDataFrame.size > 0:
            dqEltRuntimeQryTablsDataFrame.to_excel(writer,sheet_name='DQ-ELT-Runtime',startrow=1, header=False,index=False) #df1 
            worksheet = writer.sheets['DQ-ELT-Runtime'] 
            # Write the column headers with the defined format.
            for col_num, value in enumerate(dqEltRuntimeQryTablsColumns):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, 1, 18, cell_format)    
        

        # count-tally-sheet
        tableCountQryTablsDataFrame.to_excel(writer,sheet_name='Count-Difference',startrow=1, header=False,index=False)
        worksheet = writer.sheets['Count-Difference'] 
        # Write the column headers with the defined format.
        for col_num, value in enumerate(tableCountQryTablsColumns):
            worksheet.write(0, col_num, value, header_format)
            worksheet.set_column(col_num, 1, 18, cell_format)
         
        # end code for count-tally-sheet
         
        writer.save()        
        log.info("Function publish_snwflk_view_report:Sending report email to",params["email_list"])
        #Send the e-mail the xl attachement
        send_email(
            to=params["email_list"],
            subject='{env_name} Run Log {today_date}'.format(env_name=params["env_name"],today_date=today_date),
            html_content='Please find attached the {env_name} run log for {today_date}'.format(env_name=params["env_name"],today_date=today_date),
            
            files = ['/tmp/run_snowflake_report_'+now+'.xlsx']
        )
    except Exception as ex:
        print('exception found while executing SQL at Snowflake:',ex.with_traceback())
        raise ex