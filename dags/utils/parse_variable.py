import json
from airflow.models import Variable

def get_variable(key: str):
    return json.loads(Variable.get("default"))[key]
