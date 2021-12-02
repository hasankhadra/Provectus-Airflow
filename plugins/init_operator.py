from airflow.models.baseoperator import BaseOperator
import string 
from airflow.hooks.S3_hook import S3Hook

import re
"""
/\b($word)\b/i
"""
class InitMinioOperator(BaseOperator):
    """
    Reads the data from the specified file, prefilter it then distribute it equally among the Map tasks.
    """
    
    def __init__(self, data_path: str, minio_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_path = data_path
        self.minio_hook = S3Hook(minio_conn_id)
        
        if not self.minio_hook.check_for_bucket("sourcedata"):
            self.minio_hook.create_bucket("sourcedata")
        
        self.minio_hook.load_file(
            filename=data_path,
            key="tweets.csv",
            replace=True,
            bucket_name="sourcedata"
        )

    
    def pre_filter(self, data: str):
        """
        Prefilter the data. Delete all characters from the dataset except latin
        letters and spaces. Then, split the dataset into separate words by spaces.
        :return: list containing the filtered text and split into single words
        """
        
        data = data.split(",")
        
        filtered_data = []
        
        for item in data:
            new_item = ""
            
            # Filter latin letters
            for character in item:
                if character in string.ascii_lowercase or character in string.ascii_uppercase:
                    new_item += character
                else:
                    new_item += " "

            # strip and then split by spaces and endlines
            new_item = new_item.strip().split()
            
            for x in new_item:
                filtered_data.append(x)
                
        return filtered_data

    def execute(self, context):
        """
        Reads the input data. Prefilter it, then, equally distribute it among
        the mapper tasks using xcom.
        """

        data = self.minio_hook.read_key(key="tweets.csv", bucket_name="sourcedata")
        data = self.pre_filter(data)

        task_instance = context['task_instance']
    
        first_ind = 0
        for i in range(3):
            last_ind = int(((i + 1) / 3) * len(data))
            task_instance.xcom_push(f'mapper{i+1}', data[first_ind:last_ind])
            first_ind = last_ind


"""
"airflow connections add 'local_minio' --conn-type 'S3' --conn-extra {"aws_access_key_id": "minio-access-key", "aws_secret_access_key": "minio-secret-key", "host": "http://minio:9000"} ; ", "airflow connections add 'local_postgres' --conn-host 'postgres' --conn-password 'airflow' --conn-login 'airflow' --conn-type 'Postgres' ; ", 
airflow connections add 'local_minio' --conn-type 'S3' --conn-extra "{\"aws_access_key_id\": \"minio-access-key\", \"aws_secret_access_key\": \"minio-secret-key\", \"host\": \"http://minio:9000\"}"
airflow connections add 'local_postgres' --conn-host 'postgres' --conn-password 'airflow' --conn-login 'airflow' --conn-type 'Postgres'

"""