import logging
from airflow.utils.email import send_email
from utils.parse_variable import get_variable

def send_custom_email(to_email, subject, html_content, email_dryrun):
    """
    function to send email

    Args:
        to_email (list): email distribution list
        subject (string): email subject
        html_content (string): email html Body
        email_dryrun (boolean): 
    """    
    logger = logging.getLogger()
    # log level is changed because the email details were getting printed in the logs when logging level is INFO
    logger.setLevel(logging.WARN)
    send_email(to=to_email, subject=subject,
                html_content=html_content,
                dryrun=email_dryrun)
    logger.setLevel(logging.INFO)


def send_custom_failure_email(kwargs):
    """
    function to send custom failure email
    """
    try:
        logger = logging.getLogger()
        logger.info("Entering send_custom_failure_email")
        logger.info(kwargs)
        ops_environment = get_variable("ops_environment") 
        if ops_environment:
            ops_environment = ops_environment.lower()
        if (ops_environment == 'dev_ops'):
            env_name = "Non-Prod"
        else:
            env_name = "Prod"

        task_instance = kwargs.get("ti")
        to_email = (kwargs["dag"].__dict__)["default_args"]["email"]
        dag_id = task_instance.dag_id
        email_subject =  f'{env_name} Airflow Alert : {dag_id} DAG failed'
        email_message =  f'The following DAG has failed:' + '<br/><br/>'
        email_message += f'DAG ID: {dag_id}' + '<br/>'
        email_message += f'Failed task ID: {task_instance.task_id}' + '<br/>'
        email_message += f'Environment: {env_name}' + '<br/>'
        email_message += f'Timestamp: {task_instance.execution_date}' + '<br/>'
        email_message += f'Click here to go to the failed tasks: <br/> {task_instance.log_url}' 
        send_email(to=to_email,subject = email_subject,html_content=email_message)
        logger.info("Sending Failure Email")
    except Exception as ex:
        logger.error(f"Exception in send_custom_failure_email: {ex}")
        logger.error(f"{type(ex).__name__}  {__file__} {ex.__traceback__.tb_lineno}")


    