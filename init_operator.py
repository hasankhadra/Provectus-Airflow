from airflow.models.baseoperator import BaseOperator
import string 

class InitOperator(BaseOperator):
    """
    Reads the data from the specified file, prefilter it then distribute it equally among the Map tasks.
    """
    
    def __init__(self, data_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.data_path = data_path

    def pre_filter(self, data: list):
        """
        Prefilter the data. Delete all characters from the dataset except latin
        letters and spaces. Then, split the dataset into separate words by spaces.
        :return: list containing the filtered text and split into single words
        """
        
        filtered_data = []
        for row in data:

            new_row = row.split(",")
            for item in new_row:
                new_item = ""
                
                # Filter latin letters
                for character in item:
                    if character in string.ascii_lowercase or character == " " or character in string.ascii_uppercase:
                        new_item += character

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

        with open(self.data_path, "r") as file:
            data = file.readlines()
        
        task_instance = context['task_instance']
    
        first_ind = 0
        for i in range(3):
            last_ind = int(((i + 1) / 3) * len(data))
            task_instance.xcom_push(f'map{i+1}', self.pre_filter(data[first_ind:last_ind]))
            first_ind = last_ind
