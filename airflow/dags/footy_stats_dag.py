from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (RunGlueJobOperator, FootyStatsAPIOperator,
                                S3UploadOperator, DataQualityOperator)
from datetime import datetime, timedelta
import os

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
API_KEY = os.environ.get('API_KEY')

start_date = datetime(2022, 2, 4)
end_date = datetime(2022, 2, 7)

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


dag = DAG('footy_stats_dag',
          default_args=default_args,
          description='Load and transform data in AWS Glue with Airflow',
          schedule_interval='0 6 * * *',
          end_date=end_date,
          max_active_runs=1
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

get_today_matches = FootyStatsAPIOperator(
    dag=dag,
    task_id="get_today_matches",
    api_key=API_KEY,
    summary_path='/home/workspace/staging/summary/',
    detail_path='/home/workspace/staging/detail/'
)

upload_match_detail = S3UploadOperator(
    dag=dag,
    task_id="upload_match_detail",
    region='us-east-1',
    bucket='footystats-capstone',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    s3_key='input/matches/',
    local_path='/home/workspace/staging/detail/'
)    

run_glue_job = RunGlueJobOperator(
    dag=dag,
    task_id="run_glue_job",
    region='us-east-1',
    glue_job='footy-stats-etl',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET    
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    region='us-east-1',
    output_s3='s3://udacity-athena-query-output',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,    
    dq_checks=[
        {'check_sql': "SELECT count(*) FROM matches", 'db': "capstone", 'expected_result': 1},
        {'check_sql': "SELECT count(*) FROM goals", 'db': "capstone", 'expected_result': 1}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> get_today_matches
get_today_matches >> upload_match_detail
upload_match_detail >> run_glue_job
run_glue_job >> run_quality_checks
run_quality_checks >> end_operator
