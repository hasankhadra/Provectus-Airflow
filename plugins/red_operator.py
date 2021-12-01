from os import F_ULOCK
from airflow.models.baseoperator import BaseOperator
import json 
# from pgConnect import MyPgConnect

class RedOperator(BaseOperator):
    """
    Combine the results of all the mapper tasks into one frequency
    dictionary and store the results in the specified output json file.
    """
    
    def __init__(self, xcom_task_ids: list, output_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_task_ids = xcom_task_ids
        self.full_data = {}
        self.output_path = output_path
        # self.pg_client = MyPgConnect(dbname="airflow", user="airflow", password="airflow", host="localhost")

    def execute(self, context):
        """
        Pull the results of each mapper task from xcom and combine 
        them as one. Store the results in self.output_path.json
        """
        task_instance = context['task_instance']
        
        for task_id in self.xcom_task_ids:
            data = json.loads(task_instance.xcom_pull(task_ids=task_id, key=task_id))
            
            for key in data.keys():
                if key in self.full_data.keys():
                    self.full_data[key] += data[key]
                else:
                    self.full_data[key] = data[key]
 
        with open(self.output_path, 'w') as json_file:
            json.dump(self.full_data, json_file)
        

