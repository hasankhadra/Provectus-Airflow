from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.hooks.S3_hook import S3Hook

args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}
 
dag = DAG(dag_id = 'my_sample_dag', default_args=args, schedule_interval=None)
 
 
def run_this_func():
    print('I am coming first')
 
def run_also_this_func():
    print('I am coming last')
 
def write_text_file(ds, **kwargs):
    
    s3 = S3Hook('local_minio')
    s3.create_bucket("my-bucket")

with dag:

    t1 = PythonOperator(
        task_id='generate_and_upload_to_s3',
        python_callable=write_text_file,
        dag=dag
    )

    