# Project: Creating AWS Redshift DWH

This project is developed using the following concepts:

- Data modeling with PostgresSQL
- Star schema Implementation
- Development of an ETL pipeline using Python
- Infrastucture As Code (IAC) setup for spinning up an AWS Redshift CLuster
- Extracting Data from S3 to AWS Redshift

## Main Objective

The objective of this project is to create a AWS Redshift DWH Cluster for the analytics team of a fictional music streaming service called Sparkify. Sparkify is a music streaming startup with a growing user base and song database.
Their user activity and songs metadata data resides in json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, staging them in Redshift, and transforming the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

### Sample Data

The files in S3 are in json format and they are categorized as follows:

- **Song datasets**: The dataset contains  details of the songs from [Millon Songs Dataset](http://millionsongdataset.com/) in json files which are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: All the log json files are created using a [eventSimulator](https://github.com/Interana/eventsim) and they are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

### Database Schema

***Staging Tables***: Here data is loaded from S3 to Redshift staging tables:

- staging table from event data: ```staging_events``` table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `artist` | `text` | The artist name |
| `auth` | `text` | The authentication status |
| `firstName` | `text` | The first name of the user |
| `gender` | `text` | The gender of the user |
| `itemInSession` | `INTEGER` | The sequence number of the item inside a given session |
| `lastName` | `text` | The last name of the user |
| `length` | `float8` | The duration of the song |
| `level` | `text` | The level of the user´s plan (free or premium) |
| `location` | `text` | The location of the user |
| `method` | `text` | The method of the http request |
| `page` | `text` | The page that the event occurred |
| `registration` | `text` | The time that the user registered |
| `sessionId` | `INTEGER` | The session id |
| `song` | `text` | The song name |
| `status` | `INTEGER` | The status |
| `ts` | `timestamp` | The timestamp that this event occurred |
| `userAgent` | `text` | The user agent he was using |
| `userId` | `INTEGER` | The user id |

- staging table from songs data: ```staging_songs``` table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `num_songs` | `INTEGER` | The number of songs of this artist |
| `artist_id` | `text` | The artist id |
| `artist_latitude` | `float8` | The artist latitude location |
| `artist_longitude` | `float8` | The artist longitude location |
| `artist_location` | `text` | The artist descriptive location |
| `artist_name` | `text` | The artist name |
| `song_id` | `text` | The song id |
| `title` | `text` | The title |
| `duration` | `float8` | The duration of the song |
| `year` | `INTEGER` | The year of the song |


***DWH Tables***: After staging the data is ETL'd into the STAR schema to create the relational data model. 
The database is named as ```dwh``` and it contains:

- Fact Table: ```songplay``` table
- Dimension Tables: ``` songs, artists, users and time``` tables

The ```songplay``` table references the primary keys of each dimention table, enabling joins to songplays on song_id, artist_id, user_id and start_time, respectively. I have generated the database schema ER Diagram using [SchemaSpy](http://schemaspy.org/) by the following command:

```
java -jar schemaspy.jar -t pgsql -dp C:/sqljdbc4-3.0.jar -db DATABASE -host SERVER -port 5432 -s dbo -u USER -p PASSWORD -o DIRECTORY
```

![SparkifyDB Schema](schema.PNG)


## Execution Steps

To create all the tables mentioned above follow the steps mentioned below:

1. Creating the DWH config file, and save it as ```dwh.cfg``` in the project root folder. The template is as follows

```
[CLUSTER]
HOST=''
DB_NAME=dwh
DB_USER=dwhuser
DB_PASSWORD=Passw0rd
DB_PORT=5439

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[REGION]
AV_ZONE = 'us-west-2'

[DWH] 
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large
DWH_CLUSTER_IDENTIFIER=dwhCluster
DWH_IAM_ROLE_NAME=dwhRole

[AWS]
KEY=
SECRET=
```

2. Use the *ETL_end_2_end_Execution* Jupyter notebook and run till `section 1.1` to set up the needed infrastructure for this project.

3. Run the cells in `section 1.2` in the jupyter notebook to set up the database staging and analytical tables or run the following command in the cmd prompt:

    `$ python create_tables.py`
    
4. Finally, run the cells in `section 1.3` in the jupyter notebook to extract data from the files in S3, stage it in redshift, and finally store it in the Redshift DB star schema or run the following command in the cmd prompt:
    
    `$ python etl.py`
    
    
### Table Optimization using sortkeys and distkeys

The main advantage of using AWS Redshift cluster is that it automatically partitions and stores database tables on multiple slices within the cluster.
This allows for rapid and flexible scaling but it decreases the query performance because executing queries across different slices can increase copying and processing costs compared to a case where all the data is located on a single machine.

To optimize the data distribution across the cluster and to make sure the query performance does not take a hit, the Redshift's 'distKEY' and 'distsyle ALL' distribution strategies are to be used in the table design.

The 'distkey' function specifies that data in two tables with the same distkey column values will be stored on the same slice.
The 'sortkey' function allows the data to be pre sorted and inserted into the database which is helpful when using ORDER BY

Therefore, to do this optimization for the ```star schema``` designed above, the schema has been modified with the ```Distkey``` and ```sortkeys``` as follows:

I choose:

- distkey is ```artist_id``` in the ```artists``` dimension table because it is the join field with the fact table.
- ```users``` and ```songs``` dimension tables are small, therefore they are distributed by ```diststyle ALL``` on all the slices. This helps in easy ```LOOKUPS```
- ```time``` dimension table is a dim table where analysts can view pattern ordered by time therefore ```start_time``` is used as a ```sortkey```
- For the ```songplays``` fact table: ```artist_id``` -> ```distkey``` and ```start_time``` -> ```sortkey```

With these distkey and sortkey combinations I have performed some sample queries listed below

## Queries and Results

Executing the cells in `section 1.4` the following table is obtained

1. To check if the data is inserted the queries are executed
~~~ python
query1 = ("""select count(*) from songs;""")
query2 = ("""select count(*) from songplays;""")
query3 = ("""select count(*) from users;""")
query4 = ("""select count(*) from artists;""")
query5 = ("""select count(*) from time;""")
query6 = ("""select count(*) from staging_events;""")
query7 = ("""select count(*) from staging_songs;""")
queries = {'songs':query1, 'songplays':query2, 'users':query3, 'artists':query4, 'time':query5, 'staging_events':query6, 'staging_songs':query7}
for query in queries.keys():
    cur.execute(queries.get(query))
    print('Number of Records in '+query+': '+str(cur.fetchall()[0][0]))
~~~

Number of rows in each table:

| Table            | rows  |
|---               | --:   |
| staging_events   | 8056  |
| staging_songs    | 14896 |
| artists          | 10025 |
| songplays        | 333   |
| songs            | 14896 |
| time             |  8023 |
| users            |  105  |


To determine the performance of the ```DWH``` the following query was executed:

2. If the analytics team would ask the question: Q1) List the number of songs and number of artists listened by users in the 11th month in the DB

~~~ mysql
explain SELECT
  songplays.user_id,
  users.first_name,
  COUNT(songplays.songplay_id) AS song_count,
  COUNT(artists.name) AS artist_count
FROM
  songplays
  INNER JOIN songs ON songplays.song_id = songs.song_id
  INNER JOIN time ON songplays.start_time = time.start_time
  INNER JOIN artists ON songplays.artist_id = artists.artist_id
  INNER JOIN users ON songplays.user_id = users.user_id
WHERE
  time.month = 11
GROUP BY
  songplays.user_id,
  users.first_name
~~~

Then the following query plan for the cluster is created:

```
[('XN HashAggregate  (cost=146524201.02..146524202.69 rows=334 width=34)',), 
('  ->  XN Hash Join DS_DIST_ALL_NONE  (cost=3948.39..146524197.68 rows=334 width=34)',), 
('        Hash Cond: ("outer".user_id = "inner".user_id)',), 
('        ->  XN Hash Join DS_BCAST_INNER  (cost=3943.14..146524184.92 rows=333 width=25)',), 
('              Hash Cond: ("outer".start_time = "inner".start_time)',), 
('              ->  XN Seq Scan on "time"  (cost=0.00..85.16 rows=6813 width=8)',), 
('                    Filter: ("month" = 11)',), 
('              ->  XN Hash  (cost=3942.30..3942.30 rows=333 width=33)',), 
('                    ->  XN Hash Join DS_DIST_NONE  (cost=4.16..3942.30 rows=333 width=33)',), 
('                          Hash Cond: (("outer".artist_id)::text = ("inner".artist_id)::text)',), 
('                          ->  XN Seq Scan on artists  (cost=0.00..100.25 rows=10025 width=39)',), 
('                          ->  XN Hash  (cost=3.33..3.33 rows=333 width=38)',), 
('                                ->  XN Seq Scan on songplays  (cost=0.00..3.33 rows=333 width=38)',), 
('        ->  XN Hash  (cost=4.20..4.20 rows=420 width=13)',), ('              ->  XN Seq Scan on users  
(cost=0.00..4.20 rows=420 width=13)',)]
```

***Observation:*** This shows that there is only one broadcast hash inner join ```DS_BCAST_INNER``` JOIN even though 5 tables are to be joined, this is because of the optimiztaion of the table design using the sortkeys and distkeys

The query plan observed in AWS Redshift console:

![QueryPlan](query_plan.PNG)