import json
# import snowflake.connector
from utils.parse_variable import get_variable
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook

# snowflake.connector.paramstyle = 'qmark'



# -----------------------------------------------------------------------------
#   Purpose: Acquire a connection to the SnowFlake Data Warehouse
#   Exception: NONE
#   Return: Connection Object to SnowFlake DB
# -----------------------------------------------------------------------------
# def get_ssm_secret(parameter_name):
#     return ssm.get_parameter(
#         Name=parameter_name,
#         WithDecryption=True
#     )

def connect_to_snowflake():
    # snowflake_username = get_ssm_secret(f"/aas/nonprod/ops/svc_acct/sf_dw_user")
    # snowflake_password = get_ssm_secret(f"/aas/nonprod/ops/svc_acct/sf_dw_password")
    # snowflake_account = get_variable("snowflake_role")
    # snowflake_role = get_variable("snowflake_role")
    # snowflake_warehouse = get_variable("warehouse")
    # snowflake_database = get_variable("database")
    # snowflake_schema = get_variable("schema")

    # Can we parameterize the database and schema from the ops_db, and ops_schema variables
    # snowflake_connection = snowflake.connector.connect(user=snowflake_username, password=snowflake_password,
                                                       # account=snowflake_account, database=snowflake_database,
                                                       # schema=snowflake_schema, role=snowflake_role, warehouse=snowflake_warehouse)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_sm')
    return hook.get_conn()

# -----------------------------------------------------------------------------
#   Purpose: Executes the given commands in Snowflake
#   Exception: NONE
#   Return: NONE
# -----------------------------------------------------------------------------


def execute_commands(cursor, command, params):
    try:
        environment = get_variable("ops_environment")
        warehouse = get_variable("warehouse")
        schema = get_variable("cntrl_schema")
        cursor.execute("use warehouse "+warehouse)
        cursor.execute("use database "+environment)
        cursor.execute("use schema "+schema)
        result = cursor.execute(command, params).fetchall()
        return result
    except Exception as ex:
        raise ex