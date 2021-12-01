from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor

args = {
    'owner': 'pipis',
    'start_date': days_ago(1)
}
 
dag = DAG(dag_id = 'my_sample_dag', default_args=args, schedule_interval=None)
 
 
def run_this_func():
    print('I am coming first')
 
def run_also_this_func():
    print('I am coming last')
 
 
with dag:
    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_dag_create',
        region_name='us-east-1',
        bucket_name="test",
        aws_conn_id="aws_default",
        dag=dag
    )

    