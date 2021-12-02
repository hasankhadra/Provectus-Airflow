from os import F_ULOCK
from airflow.models.baseoperator import BaseOperator
import json 
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresOperator(BaseOperator):
    """
    Combine the results of all the mapper tasks into one frequency
    dictionary and store the results in the specified output json file.
    """
  
    
    def __init__(self, xcom_task_id: str, pg_conn_id: str, batch_size: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.xcom_task_id = xcom_task_id
        self.pg_hook = PostgresHook(pg_conn_id)
        self.batch_size = batch_size

    def init(self):
        """
        Initialize the connection with postgres db.
        """
        conn = self.pg_hook.get_conn()
        crsr = conn.cursor()
        return conn, crsr
    
    def create_table_frequency(self):
        """
        Creates the table frequency that we will use to store the frequency of each word in
        the tweets.csv
        """
        conn, crsr = self.init()

        crsr.execute("""
            CREATE TABLE IF NOT EXISTS frequency(
                id SERIAL PRIMARY KEY NOT NULL,
                word varchar (300) NOT NULL,
                frequency int NOT NULL
            );
        """)
        
        conn.commit()
        crsr.close()
        conn.close()
    
    def _insert_rows(self, table_name: str, words: list):
        """
        Uploads the given list of words and their frequencies into the 'frequency' table
        :param words: a list of the words to be uploaded to the 'frequency' table
        NOTE:
        the list words should contain the information about the words right after each other
        without having each word in a separate list.
        For example: words = [word1, frequency_word1, word2, frequency_word2, word3, frequency_word3, .....]
        Required columns: (word, frequency)
        """
        conn, crsr = self.init()

        values = "(" + "%s ," * 2 + ")"
        values = values[:-3] + values[-1] + ","
        values = values * (len(words) // 2)
        values = values[:-1]

        crsr.execute(
            f"INSERT INTO {table_name} (word, frequency) VALUES {values}"
            , words)

        conn.commit()
        crsr.close()
        conn.close()
    
    def _delete_table(self, table_name: str):
        """
        Deletes a table from the database
        :param table_name: the table name
        """
        conn, crsr = self.init()
        crsr.execute(f"DROP TABLE {table_name};")
        
        conn.commit()
        crsr.close()
        conn.close()
    
    def _clear_table(self, table_name: str):
        """
        Clears the table from all data.
        :param table_name: the table name
        """
        conn, crsr = self.init()
        crsr.execute(f"TRUNCATE TABLE {table_name };")

        conn.commit()
        crsr.close()
        conn.close()

    def execute(self, context):
        """
        Pull the results of each mapper task from xcom and combine 
        them as one. Store the results in self.output_path.json
        """
        task_instance = context['task_instance']
        self._delete_table('frequency')
        self.create_table_frequency()
        
        data = json.loads(task_instance.xcom_pull(task_ids=self.xcom_task_id, key="postgres_data"))
        
        list_data = []
        uploaded_rows = 0
        for key in data.keys():
            list_data.append(key)
            list_data.append(data[key])
            if len(list_data) >= self.batch_size:
                self._insert_rows('frequency', list_data)
                uploaded_rows += self.batch_size
                print("Uploaded:", uploaded_rows)
                list_data = []
        
        if len(list_data):
            self._insert_rows('frequency', list_data)
            uploaded_rows += len(list_data)
            print("Uploaded:", uploaded_rows)
        
        print("Finished uploading")

        
        
        

