from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

S3_BUCKET = 'udacity-dend'
S3_SONG_KEY = 'song_data'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-east-1'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'dag'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

#catchup false: no backfill

dag = DAG( 
        dag_id=DAG_ID,
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='@hourly',
        catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    truncate=False,
    data_format=f"JSON'{LOG_JSON_PATH}"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    region=REGION,
    truncate=True,
    data_format="JSON 'auto'"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# set execution order

# 1 - Load data
start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift

# 2 - load facts table  
stage_events_to_redshift  >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

# 3 - load facts table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# 4 - load dimension tables
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# 5 - run quality checks
run_quality_checks >> end_operator

