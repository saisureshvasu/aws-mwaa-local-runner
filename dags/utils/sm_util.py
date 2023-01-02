from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

### The steps to create this secret key can be found at: https://docs.aws.amazon.com/mwaa/latest/userguide/connections-secrets-manager.html


class SMUtil:

    _instance = None

    
    def __init__(self):
        """
        constructor function for initlizing airflow hook to access secrets manager
        """
        ### set up Secrets Manager
        hook = AwsBaseHook(client_type='secretsmanager')
        self.sm_client = hook.get_client_type('secretsmanager')
       

    
    @staticmethod
    def instance():
        """
        static method to support singleton objects

        Returns:
            SFClient
        """
        if SMUtil._instance is None:
            SMUtil._instance = SMUtil()
        return SMUtil._instance

    
    def read_from_aws_sm_fn(self, sm_secret_id_name):
        """
        fetch the secret string for the given secret name

        Args:
            sm_secret_id_name (str)

        Returns:
            SFClient
        """
        response = self.sm_client.get_secret_value(SecretId=sm_secret_id_name)
        secret_string = response["SecretString"]
        return secret_string


    
    def get_base64str_and_url(self, sm_secret_id_name):
        """
        get_base64str_and_url is utility function to get base64str, base_url from secret string

        Args:
            sm_secret_id_name (str)
        Returns:
            array of base64str, base_url 
        """
        secret_string = self.read_from_aws_sm_fn(sm_secret_id_name)
        start = secret_string.find('//') + len('//')
        end = secret_string.find('@')
        base64str = (secret_string[start:end]).split(':')[1]
        base_url = secret_string[end+1:]
        return [base64str, base_url]    

    
    def get_keys_and_values(self, sm_secret_id_name):
        """
        get_keys_and_values is utility function to return all keys and values set under the secret

        Args:
            sm_secret_id_name (str): 
        """
        pass   