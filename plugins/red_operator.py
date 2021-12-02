from os import F_ULOCK
from airflow.models.baseoperator import BaseOperator
import json 

class RedOperator(BaseOperator):
    """
    Combine the results of all the mapper tasks into one frequency
    dictionary and pass the results to the postgres operator.
    """
    
    def __init__(self, xcom_task_ids: list, **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_task_ids = xcom_task_ids
        self.full_data = {}

    def execute(self, context):
        """
        Pull the results of each mapper task from xcom and combine 
        them as one. Upload the results to XCom for postgres operator.
        """
        task_instance = context['task_instance']
        
        for task_id in self.xcom_task_ids:
            data = json.loads(task_instance.xcom_pull(task_ids=task_id, key=task_id))
            
            for key in data.keys():
                if key in self.full_data.keys():
                    self.full_data[key] += data[key]
                else:
                    self.full_data[key] = data[key]
 
        task_instance.xcom_push("postgres_data", json.dumps(self.full_data))
        

