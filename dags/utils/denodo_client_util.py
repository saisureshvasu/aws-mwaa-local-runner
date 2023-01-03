import logging
import json
import urllib3
urllib3.disable_warnings()
log = logging.getLogger(__name__)

import pyodbc
import pymssql

def test_odbc():
    #pass
    print(pyodbc.version)
    print([x for x in pyodbc.drivers()])
    server = 'zbytlm6git.database.windows.net' 
    database = 'PF_CCM_Live' 
    username = 'PF_DataTeam_read' 
    password = 'h?rx^V5?SQqkuDG5' 
    # ENCRYPT defaults to yes starting in ODBC Driver 18. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()