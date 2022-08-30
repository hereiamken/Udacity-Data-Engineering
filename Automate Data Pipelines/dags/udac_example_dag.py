from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,

}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

data_quality_checks = [
    {'check': 'select count(*) from public.songplays where userid is null',
     'expected': 0},
    {'check_sql': 'select count(*) from public.artists where name is null or artist_id is null',
     'expected_result': 0},
    {'check_sql': 'select count(*) from public.songs where title is null or song_id is null',
     'expected_result': 0},
    {'check_sql': 'select count(*) from public.users where first_name is null or userid is null',
     'expected_result': 0},
    {'check_sql': 'select count(*) from public."time" where weekday is null or ',
     'expected_result': 0},
    {'check_sql': 'select count(*) from public.songplays sp inner join public.users u on u.userid = sp.userid where u.userid is null',
     'expected_result': 0}
]

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='staging_events',
    s3_bucket='udacity-dend',
    s3_path='log_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region='us-west-2',
    copy_json_option='s3://' + 'udacity-dend' + '/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    s3_bucket='udacity-dend',
    s3_path='song_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region='us-west-2',
    copy_json_option='auto',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='songplays',
    query_sql=SqlQueries.songplay_table_insert,
    truncate_table_flag='Y'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='users',
    query_sql=SqlQueries.user_table_insert,
    truncate_table_flag='Y'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='songs',
    query_sql=SqlQueries.song_table_insert,
    truncate_table_flag='Y'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='artists',
    query_sql=SqlQueries.artist_table_insert,
    truncate_table_flag='Y'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='time',
    query_sql=SqlQueries.time_table_insert,
    truncate_table_flag='Y'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    data_quality_check=data_quality_checks,
    redshift_conn_id='redshift',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
