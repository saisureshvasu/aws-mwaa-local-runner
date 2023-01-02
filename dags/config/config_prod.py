default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'email': ['SnowflakeNotifications@pfhq.com'],
            'email_on_failure': True,
            
        }
catchup=False
max_active_runs=1