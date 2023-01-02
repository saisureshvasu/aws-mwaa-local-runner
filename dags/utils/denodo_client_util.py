import logging
import json
import urllib3
urllib3.disable_warnings()
log = logging.getLogger(__name__)

import pyodbc


def test_odbc():
   
    print([x for x in pyodbc.drivers() if x.endswith(' for SQL Server')])
    