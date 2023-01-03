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
    