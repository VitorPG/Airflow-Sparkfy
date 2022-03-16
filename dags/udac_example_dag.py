from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retries_delay': timedelta(minutes=1),
    'email_on_retry': False,
    'catchup':False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'udacity-dend',
    s3_key='log_data',
    json_format='s3://udacity-dend/log_json_path.json'    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_format='auto'

)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn='redshift',
    sql=SqlQueries.songplays_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    overwrite=True,
    table='users',
    redshift_conn='redshift',
    sql=SqlQueries.users_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    overwrite=True,
    table='songs',
    redshift_conn='redshift',
    sql=SqlQueries.songs_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    overwrite=True,
    table='artists',
    redshift_conn='redshift',
    sql=SqlQueries.artists_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    overwrite=True,
    table='time',
    redshift_conn='redshift',
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn='redshift',
    #tables=['users','time','songplays','songs','artists'],
    quality_check= [
            {'check_type': 'null_records', 
             'table':'users',
             'quality_check':'SELECT COUNT(*) FROM users WHERE userid is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'songplays',
             'quality_check':'SELECT COUNT(*) FROM songplays WHERE playid is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'songs',
             'quality_check':'SELECT COUNT(*) FROM songs WHERE songid is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'time',
             'quality_check':'SELECT COUNT(*) FROM time WHERE start_time is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'artists',
             'quality_check':'SELECT COUNT(*) FROM artists WHERE artistid is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type':'qnt_records',
             'table':'users',
             'quality_check':'SELECT COUNT(*) FROM users',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'songs',
             'quality_check':'SELECT COUNT(*) FROM songs',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'artists',
             'quality_check':'SELECT COUNT(*) FROM artists',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'songplays',
             'quality_check':'SELECT COUNT(*) FROM songplays',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'time',
             'quality_check':'SELECT COUNT(*) FROM time',
             'expected_value':1,
             'comparison': '<'}
            ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator>>stage_songs_to_redshift
start_operator>>stage_events_to_redshift
stage_songs_to_redshift>>load_songplays_table
stage_events_to_redshift>>load_songplays_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table
load_user_dimension_table>>run_quality_checks
load_song_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks
run_quality_checks>>end_operator