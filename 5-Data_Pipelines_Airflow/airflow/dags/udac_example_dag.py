from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTableOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries,CreateSqlTables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
} # 'end_date': datetime(2018, 11, 2),

dag = DAG('Project_5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_empty_tables = CreateTableOperator(
    task_id='Create_Tables',
    dag=dag,
    redshift_conn_id="redshift",
    create_tables = [CreateSqlTables.create_staging_events, CreateSqlTables.create_staging_songs,
                     CreateSqlTables.create_users, CreateSqlTables.create_songs,CreateSqlTables.create_artists,
                     CreateSqlTables.create_time, CreateSqlTables.create_songplays]
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    jsonpaths="s3://udacity-dend/log_json_path.json",
    delimiter="",
    ignore_headers=0,
    data_files_format="json",
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A", # technically "song_data" but it is taking too long to complete so only uploading songs from one folder
    delimiter="",
    ignore_headers=0,
    data_files_format="json",
    provide_context=True,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    destination_fact_table = "songplays",
    sql_statement = SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,                 
    redshift_conn_id = "redshift",
    destination_dim_table = "users",
    sql_statement = SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,    
    redshift_conn_id = "redshift",
    destination_dim_table = "songs",
    sql_statement = SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    destination_dim_table = "artists",
    sql_statement = SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    destination_dim_table = "time",
    sql_statement = SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables_list=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_empty_tables
create_empty_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
