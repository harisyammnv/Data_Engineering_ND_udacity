# ETL Process for creating a Relation Data Model with PostgresSQL for Sparkify

First project submission for the Udacity's Data Engineering Nanodegree (DEND).

This project is developed using the following concepts:

- Data modeling with PostgresSQL
- Database model is star schema 
- Development of an ETL pipeline using Python

# Main Objective

The objective of this project is to create a PostgresSQL analytics database for a fictional music streaming service called Sparkify. Sparkify's analytics team seeks to understand what, when and how users are playing songs on the company's music app. The analysts need an easy way to query and analyze the songplay data, which is currently stored in raw JSON logs and metadata files on a local directory.

In this project, I have implemented an ETL pipeline using python to process and upload the data into a PostgreSQL database. The ETL process extracts each songplay from the list of page actions recorded by the app. The database schema implemented is a STAR schema which is prefered when read intensive queries are perforemed for creating various analytics/statistics

### Sample Data
- **Song datasets**: The dataset contains  details of the songs from [Millon Songs Dataset](http://millionsongdataset.com/) in json files which are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: All the log json files are created using a [eventSimulator](https://github.com/Interana/eventsim) and they are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```


### Database Schema

I have used a STAR schema to create the relational data model. The database is named as ```sparkifydb``` and it contains:

- Fact Table: ```songplay``` table
- Dimension Tables: ``` songs, artists, users and time``` tables

The ```songplay``` table references the primary keys of each dimention table, enabling joins to songplays on song_id, artist_id, user_id and start_time, respectively. I have generated the database schema ER Diagram using [SchemaSpy](http://schemaspy.org/) by the following command:

```
java -jar schemaspy.jar -t pgsql -dp C:/sqljdbc4-3.0.jar -db DATABASE -host SERVER -port 5432 -s dbo -u USER -p PASSWORD -o DIRECTORY
```

![SparkifyDB Schema](schema.PNG)


### ETL process

Prerequisites: 
- Sparkify Database and all the empty tables should be created using ```create_tables.py``` python script

1. ```etl.py```: connects to the ```sparkifydb``` database, and begins by processing the jsons for inserting all song related data.

2. ```def process_data(cur, conn, filepath, func):``` uses the tree file structure under ```/data/song_data```, and for each json file encountered the file is send to another function called ```process_song_file(cur, filepath)```.

3. Each file is loaded as a dataframe using a pandas function called ```read_json()```.

4. For each row in the dataframe the fields which are interested are selected for insertions into the respective tables:
    
    ```
    song_data table requires [song_id, title, artist_id, year, duration]
    ```
    ```
    artist_data table requires [artist_id, artist_name, artist_location, artist_longitude, artist_latitude]
    ```

5. Once all files from song_data are read and processed, the log_data found at the folder ```/data/log_data/``` is processed.

6. From step 2, the function ```process_data``` will send the file path to the function ```process_log_file``` where the data is loaded into a pandas dataframe using ```read_json()```.

7. The rows where ```page = 'NextSong'``` only is selected according to the project description.

8. The ```ts``` column is in the format ```unix_time (in milliseconds)```  which is converted to pandas datetime format. This format will help in extracting parameters needed for the ```time``` table where the following ```[date, day, hour, week, weekday]``` are to be inserted.

10. Next the ```user``` data table is populated with the ```[user_id,first_name,last_name,gender,level]``` which are extracted from the dataframe

11. Finally the lookup of song and artist ids from their tables is performed by specifying the song name, artist_name and song duration that is present in the song play data. The query used is the following:
    ```
    song_select = ("""
        SELECT song_id, artists.artist_id
        FROM songs JOIN artists ON songs.artist_id = artists.artist_id
        WHERE songs.title = %s
        AND artists.aritst_name = %s
        AND songs.duration = %s
    """)
    ```
12. The ```songplay_id``` in the ``` songplay``` table is created using a reproducible ```UUID``` created from the combination of ```song_name + start_time + userId```.

13. The last step is inserting the data into ```songplay``` fact table.


### Summary Steps to Reproduce the Results

1. Execute ```create_tables.py``` from ```terminal``` or ```python console``` using ``` python creat_tables.py ``` to set up database and tables.
2. Execute ```etl.py``` from terminal or console using ```python etl.py``` to process and load data into database.
3. Launch ```etl.ipynb``` using ```Jupyter Notebook``` to explore how process was developed.
4. Launch ```test.ipynb``` to run validation and check out example queries.


### Sample Queries for analytics

- Query-1: Gender-Level Distribution

~~~ mysql
> %sql SELECT level,gender,COUNT(level) FROM users GROUP BY (level,gender);
~~~

- Output for Query-1
```
```
| level | gender| count  |
|-------|-------|--------|
| free  | F     |  40    |
| free  | M     |  34    |
| paid  | F     |  15    |
| paid  | M     |  7     |


- Query-2: NUmber of songplays in November 2018

~~~ mysql
> %sql SELECT time.month, time.year, COUNT(sp.songplay_id) as songplay_count FROM songplays sp LEFT JOIN time ON sp.start_time = time.start_time GROUP BY time.month, time.year;
~~~

- Output for Query-2
```
```

| month | year  | count  |
|-------|-------|--------|
| 11    | 2018  |  6820  |