from datetime import timedelta, datetime
from textwrap import dedent
from init_operator import InitMinioOperator

from airflow import DAG
from airflow.utils.dates import days_ago

from map_operator import MapOperator
from red_operator import RedOperator
from postgres_operator import PostgresOperator

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
    'WordCount',
    default_args=default_args,
    description='MapReduce for frequency count',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2019, 10, 13, 15, 50),
    catchup=False
) as dag:
    
    dag.doc_md = """
    This dag is responsible for calculating the frequency of each word
    inside a provided dataset using the MapReduce paradigm. It consists of
    - 1 initializer task
    - 3 mapper tasks
    - 1 reducer task
    """  

    initializer = InitMinioOperator(
        task_id='init_minio',
        data_path=DOCKER_INPUT,
        minio_conn_id="local_minio"
    )

    initializer.doc_md = dedent("""
    This task is responsible for the initialization phase. It reads the data from 
    the specified file, prefilter it then distribute it equally among the Map tasks.
    """)

    mapper1 = MapOperator(
        task_id='mapper1',
        xcom_task_id = 'init_minio'
    )
    
    mapper1.doc_md = dedent("""
    This task is responsible for mapping the data received from the initializer. It counts the frequency
    of each word in the data and saves it as a python dictionary. Then, it passes this frequency
    dictionary to the reducer task.
    """)

    mapper2 = MapOperator(
        task_id='mapper2',
        xcom_task_id = 'init_minio'
    )
    
    mapper2.doc_md = dedent("""
    This task is responsible for mapping the data received from the initializer. It counts the frequency
    of each word in the data and saves it as a python dictionary. Then, it passes this frequency
    dictionary to the reducer task.
    """)

    mapper3 = MapOperator(
        task_id='mapper3',
        xcom_task_id = 'init_minio'
    )
    
    mapper3.doc_md = dedent("""
    This task is responsible for mapping the data received from the initializer. It counts the frequency
    of each word in the data and saves it as a python dictionary. Then, it passes this frequency
    dictionary to the reducer task.
    """)
    
    reducer = RedOperator(
        task_id='reducer',
        xcom_task_ids=['mapper1', 'mapper2', 'mapper3']
    )
    reducer.doc_md = dedent("""
    This task is responsible for combining the results of all the mapper tasks into one frequency
    dictionary and then stores the results in the specified output json file.
    """)

    postgres_uploader = PostgresOperator(
        task_id="postgres",
        xcom_task_id="reducer",
        pg_conn_id = "local_postgres",
        batch_size = 1000
    )
    
    # These are the dependencies between the tasks inside this dag
    # We first run the initializer, then distribute the load among
    # the mapper tasks, and only after all the mappers finish,
    # the reducer take it from there and combine the results of
    # all the mappers
    initializer >> [mapper1, mapper2, mapper3] >> reducer >> postgres_uploader
    
    
