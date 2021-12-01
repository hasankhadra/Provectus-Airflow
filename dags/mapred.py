from datetime import timedelta
from textwrap import dedent
from init_operator import InitOperator

from airflow import DAG
from airflow.utils.dates import days_ago

from map_operator import MapOperator
from red_operator import RedOperator

LOCAL_INPUT = "./tweets.csv"
LOCAL_OUTPUT = "./output.json"

DOCKER_INPUT = '/opt/airflow/dags/tweets.csv'
DOCKER_OUTPUT = '/opt/airflow/dags/output.json'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'FrequencyCount',
    default_args=default_args,
    description='MapReduce for frequency count',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    dag.doc_md = """
    This dag is responsible for calculating the frequency of each word
    inside a provided dataset using the MapReduce paradigm. It consists of
    - 1 initializer task
    - 3 mapper tasks
    - 1 reducer task
    """  

    initializer = InitOperator(
        task_id='init',
        data_path=LOCAL_INPUT
    )

    initializer.doc_md = dedent("""
    This task is responsible for the initialization phase. It reads the data from 
    the specified file, prefilter it then distribute it equally among the Map tasks.
    """)

    mapper1 = MapOperator(
        task_id='map1',
        xcom_task_id = 'init'
    )
    
    mapper1.doc_md = dedent("""
    This task is responsible for mapping the data received from the initializer. It counts the frequency
    of each word in the data and saves it as a python dictionary. Then, it passes this frequency
    dictionary to the reducer task.
    """)

    mapper2 = MapOperator(
        task_id='map2',
        xcom_task_id = 'init'
    )
    
    mapper2.doc_md = dedent("""
    This task is responsible for mapping the data received from the initializer. It counts the frequency
    of each word in the data and saves it as a python dictionary. Then, it passes this frequency
    dictionary to the reducer task.
    """)

    mapper3 = MapOperator(
        task_id='map3',
        xcom_task_id = 'init'
    )
    
    mapper3.doc_md = dedent("""
    This task is responsible for mapping the data received from the initializer. It counts the frequency
    of each word in the data and saves it as a python dictionary. Then, it passes this frequency
    dictionary to the reducer task.
    """)
    
    reducer = RedOperator(
        task_id='red1',
        xcom_task_ids=['map1', 'map2', 'map3'],
        output_path=LOCAL_OUTPUT
    )
    reducer.doc_md = dedent("""
    This task is responsible for combining the results of all the mapper tasks into one frequency
    dictionary and then stores the results in the specified output json file.
    """)

    # These are the dependencies between the tasks inside this dag
    # We first run the initializer, then distribute the load among
    # the mapper tasks, and only after all the mappers finish,
    # the reducer take it from there and combine the results of
    # all the mappers
    initializer >> [mapper1, mapper2, mapper3] >> reducer
    
    
