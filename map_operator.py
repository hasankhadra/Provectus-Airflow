from airflow.models.baseoperator import BaseOperator
import json

class MapOperator(BaseOperator):
    """
    Map the data received from the initializer. Counts the frequency of each word 
    in the data and save it as a python dictionary. Then, pass this frequency
    dictionary to the reducer task.
    """
    
    def __init__(self, xcom_task_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_task_id = xcom_task_id

    def map_data(self, data: list):
        """
        Calculate the frequencey of each word inside the data
        passed from the initializer.

        Args:
            data (list): data from initializer task 

        Returns:
            [dict]: dictionary containing the frequency of each word in data
        """
        mapping = {}
        for item in data:
            if mapping.get(item):
                mapping[item] += 1
            else:
                mapping[item] = 1
        return mapping

    def execute(self, context):
        """
        Computes the frequency of each word in the data and then pushes it
        to xcom, where the reducer task will pull it from.
        """
        
        task_instance = context['task_instance']
        
        data = task_instance.xcom_pull(task_ids=self.xcom_task_id, key=self.task_id)
        mapped_data = self.map_data(data)
        
        task_instance.xcom_push(self.task_id, json.dumps(mapped_data))
        
        