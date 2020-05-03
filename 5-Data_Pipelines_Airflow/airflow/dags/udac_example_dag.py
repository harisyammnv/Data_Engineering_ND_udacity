from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CreateTableOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries,CreateSqlTables

"""
This Python file creates the complete Dag Definition in Airflow
for performing ETL from the udacity-dend bucket to extract
sparkify's Log Data and Songs data
"""

"""
If end time is needed uncomment the end_date and adjust the
start_date accordingly
"""
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}  # 'end_date': datetime(2018, 11, 2),

"""
A dag's options are to be defined below
I have used max_active_runs = 1, but this can be 
removed to execute in parallel
"""

dag = DAG('Project_5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

"""
Empty Tables step was added by me to ensure that this process can be seen in 
the ETL, it creates Tables if they don't exist in AWS Redshift otherwise it does not
do anything. But if the DB is cleared after every run in the staging it is better to 
have it
"""

create_empty_tables = CreateTableOperator(
    task_id='Create_Tables',
    dag=dag,
    redshift_conn_id="redshift",
    create_tables = [CreateSqlTables.create_staging_events, CreateSqlTables.create_staging_songs,
                     CreateSqlTables.create_users, CreateSqlTables.create_songs,CreateSqlTables.create_artists,
                     CreateSqlTables.create_time, CreateSqlTables.create_songplays]
)



"""
Staging Events step --> Copying Data from AWS S3 to Redshift
you could technically have log_data/{execution_date.year}/{execution_date.month}
if there are multiple sub folders for each month and year
"""

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

"""
Staging songs --> This function takes a lot of time to copy songs
data from AWS S3 to redhsift

to minimize running time and to check the complete run of the whole dag
quickly use "s3_key = song_data/A/A/A" to just copy songs from one folder

"""

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table_name="staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data", # technically "song_data" but it is taking too long to complete so only uploading songs from one folder
    delimiter="",
    ignore_headers=0,
    data_files_format="json",
    provide_context=True,
)

"""
Loading Songplays Fact Table by using the queries in the helpers function
We use Star Schema here. please look at the README of this repo for the
schema of the DB in AWS Redshift
"""

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    destination_fact_table = "songplays",
    sql_statement = SqlQueries.songplay_table_insert,
)

"""
Loading User Dimension Table
"""
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,                 
    redshift_conn_id = "redshift",
    destination_dim_table = "users",
    sql_statement = SqlQueries.user_table_insert,
)

"""
Loading Song Dimension Table
"""

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,    
    redshift_conn_id = "redshift",
    destination_dim_table = "songs",
    sql_statement = SqlQueries.song_table_insert,
)

"""
Loading Artists Dimension Table
"""

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    destination_dim_table = "artists",
    sql_statement = SqlQueries.artist_table_insert,
)

"""
Loading Time Dimension Table
"""

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    destination_dim_table = "time",
    sql_statement = SqlQueries.time_table_insert,
)

"""
Data Quality checks are executed after loading the tables into Redshift
checks for empty tables
"""

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
