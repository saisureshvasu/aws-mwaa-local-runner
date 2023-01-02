import boto3
from botocore.client import Config
s3 = boto3.resource('s3')


class S3ClientUtil:

    _instance = None


    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        """
        constructor function for all salesforce api requests. initializes s3_client attribute

        Args:
            aws_access_key_id (string, optional): AWS access key. Defaults to None.
            aws_secret_access_key (string, optional): AWS secert key. Defaults to None.
        """
        if aws_access_key_id is not None and aws_secret_access_key is not None:
            self.s3_client = boto3.client('s3', config=Config(signature_version='s3v4'),
                                          aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key)
        else:
            self.s3_client =  boto3.client('s3')


    @staticmethod
    def instance():
        """
        static method to support singleton client objects

        Returns:
            S3Client
        """
        if S3ClientUtil._instance is None:
            S3ClientUtil._instance = S3ClientUtil()
        return S3ClientUtil._instance

    #### UTILITY FUNCTIONS ####

   
    def write_data(self, bucket_name, prefix, data):
        """
        writes data into the given bucket

        Args:
            bucket_name (string): S3 bucket name
            prefix (string): s3 key prefix
            data (bytes): data to write to s3
        """
        self.s3_client.put_object(Bucket=bucket_name, Body=data, Key=prefix)
    
    
    def download_file(self, s3_bucket, s3_object, local_path):
        """
        downloads the file to a local path

        Args:
            s3_bucket (string): s3 bucket name
            s3_object (string): s3 object name
            local_path (string): local path to download file
        """
        self.s3_client.download_file(s3_bucket, s3_object, local_path)


    def get_all_objects(self, bucket_name, prefix, exclude_key=""):
        """
        Returns list of all objects in the given bucket

        Args:
            bucket_name (string): s3 bucket name
            prefix (string): object prefix
            exclude_key (str, optional): exclude regex . Defaults to "".

        Returns:
            list
        """
        continuation_token = ""
        list_of_objects = []
        while (continuation_token is not None):
            args = {
                "Bucket": bucket_name,
                "Prefix": prefix,
            }
            if continuation_token != "":
                args['ContinuationToken'] = continuation_token

            object_json = self.s3_client.list_objects_v2(**args)
            list_of_objects.extend([(key['Key'], key['LastModified']) for key in object_json.get('Contents', []) if
                                    key['Key'].split("/")[-1] != exclude_key])
            continuation_token = object_json.get('NextContinuationToken', None)
        return list_of_objects

    
    def move_files(self, bucket_name, src_key, tgt_bucket, tgt_key):
        """
        moves files from src bucket to target

        Args:
            bucket_name (str): s3 bucket name
            src_key (str): s3 src key name
            tgt_bucket (str): s3 tgt bucket name
            tgt_key (str): s3 tgt key name
        """
        copy_source = {
            'Bucket': bucket_name,
            'Key': src_key
        }
        self.s3_client.copy(copy_source, tgt_bucket, tgt_key)
        self.s3_client.delete_object(Bucket=bucket_name, Key=src_key)

    
    def get_file_name(self, bucket_name, prefix):
        """
        retrives file name for matching file prefixes from the bucket

        Args:
            bucket_name (str): s3 bucket name
            prefix (str): s3 key name

        Returns:
            str: filename
        """
        files = self.s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
        return files["Contents"][0]["Key"]

    
    def get_object_list(self, bucket_name, prefix):
        """
         writes list of all objects in the bucket

        Args:
            bucket_name (str): s3 bucket name
            prefix (str): s3 key name

        Returns:
            list: list of keys inside the bucket and path
        """
        return list(map(lambda x: x[0].split('/')[-1], self.get_all_objects(bucket_name, prefix)))

    # -----------------------------------------------------------------------------------------------------------
    #   Purpose: 
    #   Exception: NONE
    #   Return: NONE
    # ----------------------------------------------------------------------------------------------------------- 
    def read_object_in_bytes(self, bucket_name, prefix):
        """
        returns file contents in bytes

        Args:
            (str): s3 bucket name
            prefix (str): s3 key name

        Returns:
            str: file contents
        """
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=prefix)
        return obj['Body'].read()