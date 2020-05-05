# Project: Data Pipelines with Airflow

This project is developed using the following concepts:

- Development of an Automated ETL pipeline using Python and Airflow
- Extracting Data from AWS S3 to AWS Redshift for staging
- Data modeling with PostgresSQL (AWS Redshift)
- Star schema Implementation

## Main Objective

The objective of this project is to create an ```Airflow Data Pipeline``` for the analytics team of a fictional music streaming service called Sparkify. Sparkify is a music streaming startup with a growing user base and song database.
Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline in ```Airflow``` that 
extracts their data from AWS S3, stages them in AWS Redshift, and transforming the data into a set of dimensional tables in AWS Redshift. The complete process
is to be developed in Airflow using ```custom airflow operators``` which would basically create an ```automated ETL Pipeline``` which can be scheduled when needed

## Files in Repository

- [/airflow/dags/udac_example_dag.py](/airflow/dags/udac_example_dag.py) -- Main DAG definition file. Uses custom defined Operators to create empty tables if they dont exists, 
stage data to redshift, populate the data warehouse and run data quality checks.

- [/airflow/plugins/operators/create_tables.py](/airflow/plugins/operators/create_tables.py) -- ```CreateTableOperator``` Operator plugin created from the ```create_tables.sql``` file to create empty tables in AWS Redshift

- [/airflow/plugins/operators/stage_redshift.py](/airflow/plugins/operators/stage_redshift.py) -- ```StageToRedshiftOperator``` loads data from S3 to staging tables in redshift. User may specify csv or JSON file format. CSV file options include ```delimiter``` and whether to ignore ```headers```. JSON options include automatic parsing or use of ```JSONpaths file``` in the COPY command

- [/airflow/plugins/operators/load_fact.py](/airflow/plugins/operators/load_fact.py) -- ```LoadFactOperator``` appends data from staging tables into the main fact table of the star schema in redshift

- [/airflow/plugins/operators/load_dimension.py](/airflow/plugins/operators/load_dimension.py) -- ```LoadDimensionOperator``` loads data into dimension tables from staging tables. Update mode can be set to 'insert' or 'overwrite'

- [/airflow/plugins/operators/data_quality.py](/airflow/plugins/operators/data_quality.py) -- ```DataQualityOperator```performs data quality checks at the end of the pipeline run. This basically checks if the number of rows in the table are empty or not  


## The ETL DAG

This ETL DAG shows the ETL pipeline flow from creating the Tables in the AWS Redshift to perfroming data quality checks for 
each dag run

![graph_view](https://github.com/harisyammnv/Data_Engineering_ND_udacity/blob/master/5-Data_Pipelines_Airflow/images/graph_view.PNG)

A few successful dag runs would like the below in the ```Airflow UI```
![treeview](https://github.com/harisyammnv/Data_Engineering_ND_udacity/blob/master/5-Data_Pipelines_Airflow/images/treeview.PNG)

### Database Schema

***Staging Tables***: Here data is loaded from S3 to Redshift staging tables:

- staging table from event data: ```staging_events``` table
- staging table from songs data: ```staging_songs``` table

***DWH Tables***: The Star Schema can be created and can be filled with the above ETL pipeline.
The fact table is used to store all user song activities with the category "NextSong". Using this table, the analysts can analyze the app performance with the dimensions users, songs, artists and time.
After staging the data is ETL'd into the STAR schema to create the relational data model. 
The database is named as ```dwh``` and it contains:

- Fact Table: ```songplay``` table
- Dimension Tables: ``` songs, artists, users and time``` tables

The ```songplay``` table references the primary keys of each dimention table, enabling joins to songplays on song_id, artist_id, user_id and start_time, respectively. I have generated the database schema ER Diagram using [SchemaSpy](http://schemaspy.org/) by the following command:

```
java -jar schemaspy.jar -t pgsql -dp C:/sqljdbc4-3.0.jar -db DATABASE -host SERVER -port 5432 -s dbo -u USER -p PASSWORD -o DIRECTORY
```

![treeview](https://github.com/harisyammnv/Data_Engineering_ND_udacity/blob/master/5-Data_Pipelines_Airflow/images/schema.png)

### Dataset preview from S3

The files in S3 are in json format and they are categorized as follows:

- **Song datasets**: The dataset contains  details of the songs from [Millon Songs Dataset](http://millionsongdataset.com/) in json files which are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: All the log json files are created using a [eventSimulator](https://github.com/Interana/eventsim) and they are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```
