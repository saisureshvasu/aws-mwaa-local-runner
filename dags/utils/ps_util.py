import boto3

class PSUtil:
    _instance = None

    def __init__(self):
        """
        Constructor to get Parameter Store object
        """
        self.ps_client = boto3.client("ssm")

    @staticmethod
    def instance():
        """
        static method to support singleton objects
        INPUT: None

        RETURNS:  PSUtil Object
        """
        if PSUtil._instance is None:
            PSUtil._instance = PSUtil()
        return PSUtil._instance


    def get_ssm_secret(self,parameter_name,decryption=True):
        """
        Read the value from parameter store and decrypt.

        INPUT: Parameter_name: string, decryption: Boolean

        RETURNS: Parameter value
        """
        return self.ps_client.get_parameter(
            Name=parameter_name,
            WithDecryption=decryption
        ).get("Parameter").get("Value")