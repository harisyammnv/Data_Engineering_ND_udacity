import configparser
import logging
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from datetime import datetime
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')


#Create and configure logger 
#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, 
#                    format='%(asctime)s - %(levelname)s - %(message)s', 
#                    filemode='w') 
#Creating an object 
#logger=logging.getLogger() 


print("{} : Reading in the AWS credentials".format(datetime.now()))
if config['AWS']['AWS_ACCESS_KEY_ID']!='':
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
else:
    print("{} : AWS KEY ID is not present... Exiting!!".format(datetime.now()))
    sys.exit()
if config['AWS']['AWS_SECRET_ACCESS_KEY']!='':
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
else:
    print("{} : AWS SECRET KEY is not present... Exiting!!".format(datetime.now()))
    sys.exit()

    
def create_extract_select_exprs(json_cols: list, schema_cols: list, extract_col='') -> list:
    if extract_col!='':
        exprs = ["{}({}) as {}".format(dfc, extract_col,sc) for dfc, sc in zip(json_cols, schema_cols)]
    else:
        exprs = ["{} as {}".format(dfc,sc) for dfc, sc in zip(json_cols, schema_cols)]
    return exprs


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_song_data(spark, input_data, output_data):
    
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    print("{} : Reading Song Data from input S3 Folder".format(datetime.now()))
    # read song data file
    df = spark.read.json(song_data)
    print("{} : Finished Reading Song Data !!!".format(datetime.now()))
    
    print("{} : Creating Songs Dimension Table".format(datetime.now()))
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    print("{} : Creating Songs Table Completed !!!".format(datetime.now()))
    
    # write songs table to parquet files partitioned by year and artist
    print("{} : Writing Songs table to S3".format(datetime.now()))
    songs_table.write.parquet(os.path.join(output_data, 'songs'), mode='overwrite', partitionBy=["year", "artist_id"])
    print("{} : Writing Songs Table to S3 Completed !!!".format(datetime.now()))
    
    print("{} : Creating Artists Dimension Table".format(datetime.now()))
    # extract columns to create artists table
    artists_table = df.selectExpr(create_extract_select_exprs(["artist_id", "artist_name", "coalesce(nullif(artist_location, ''), 'N/A')", 
                                                       "coalesce(artist_latitude, 0.0)", "coalesce(artist_longitude, 0.0)"],
                                                      ['artist_id', 'name', 'location', 'latitude', 'longitude'])).distinct()
    print("{} : Creating Artists Table Completed !!!".format(datetime.now()))
    
    print("{} : Writing Artists table to S3".format(datetime.now()))
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), mode='overwrite')
    print("{} : Writing Artists table to S3 Completed !!!".format(datetime.now()))

def process_log_data(spark, input_data, output_data):
    
    print("{} : Reading Log Data from input S3 Folder".format(datetime.now()))
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*.json')

    # read log data file
    df = spark.read.format("json").load(log_data)
    print("{} : Finished Reading Log Data !!!".format(datetime.now()))
    
    # filter by actions for song plays
    df = df.filter(df["page"]=='NextSong')
    
    print("{} : Creating Users Dimension Table".format(datetime.now()))
    # extract columns for users table    
    users_table = df.selectExpr(create_extract_select_exprs(['userId', 'firstName', 'lastName', 'gender', 'level'], 
                                                         ['user_id', 'first_name', 'last_name', 'gender', 'level'])).dropDuplicates()
    print("{} : Creating Users Table Completed !!!".format(datetime.now()))
    
    print("{} : Writing Users table to S3".format(datetime.now()))
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), mode='overwrite')
    print("{} : Writing Users table to S3 Completed !!!".format(datetime.now()))

    # create timestamp column from original timestamp column
    print("{} : Creating Time Dimension Table".format(datetime.now()))
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("date_time", get_timestamp(df['ts']))
    time_exprs = ['date_time as start_time'] + create_extract_select_exprs(['hour', 'dayofmonth', 'weekofyear', 'month', 'year', 'dayofweek'],
                                                                           ['hour', 'day', 'week', 'month', 'year', 'weekday'],'date_time')
    # extract columns to create time table
    time_table = df.selectExpr(time_exprs).distinct()
    print("{} : Creating Time Table Completed !!!".format(datetime.now()))
    
    print("{} : Writing Time table to S3".format(datetime.now()))
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'), mode='append', partitionBy=["year", "month"])
    print("{} : Writing Time table to S3 Completed !!!".format(datetime.now()))

    # read in song data to use for songplays table
    print("{} : Reading Songs Table from S3".format(datetime.now()))
    songs_path = os.path.join(output_data, 'songs')
    song_df = spark.read.parquet(songs_path)
    print("{} : Loading of Songs Table from S3 Completed !!!".format(datetime.now()))
    song_df = song_df.withColumnRenamed("artist_id", "songs_artist_id")
    
    print("{} : Reading Artists Table from S3".format(datetime.now()))
    artists_path = os.path.join(output_data, 'artists')
    artist_df = spark.read.parquet(artists_path)
    print("{} : Loading of Artists Table from S3 Completed !!!".format(datetime.now()))
    artist_df = artist_df.withColumnRenamed("artist_id", "artists_artist_id").withColumnRenamed("location", "artist_location")
    
    print("{} : Creating songplays Fact Table using songs and artist table".format(datetime.now()))
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(df.date_time.alias("start_time"), 
                             df.userId.alias("user_id"), 
                             "level", 
                             "song", 
                             "artist", 
                             df.sessionId.alias("session_id"), 
                             "location",
                             df.userAgent.alias("user_agent")) \
                    .join(song_df, df.song==song_df.title, 'left_outer')   \
                    .join(artist_df, df.artist==artist_df.name, 'left_outer') \
                    .selectExpr("start_time",
                                "user_id",
                                "level",
                                "song_id",
                                "coalesce(artists_artist_id, songs_artist_id) as artist_id",
                                "session_id",
                                "location",
                                "user_agent",
                                "year(start_time) as year",
                                "month(start_time) as month") \
                    .dropDuplicates() \
                    .withColumn('songplay_id', F.monotonically_increasing_id()) 
    print("{} : Creating songplays Fact Table Completed !!!".format(datetime.now()))
    
    # write songplays table to parquet files partitioned by year and month
    print("{} : Writing songplays table to S3".format(datetime.now()))
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), mode='overwrite', partitionBy=["year", "month"])
    print("{} : Writing songplays table to S3 Completed !!!".format(datetime.now()))

def main():
    print(100*"*")
    print("{} : Creating Spark Session".format(datetime.now()))
    spark = create_spark_session()
    print("{} : Spark Session Created !!!".format(datetime.now()))
    print(100*"*")

    print("{} : Pyspark script logger initialized".format(datetime.now()))

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-spark-hari/"
    print(100*"*")
    print("{} : Processing Song Data from input Folder".format(datetime.now()))
    process_song_data(spark, input_data, output_data)
    print("{} : Song Data Processed".format(datetime.now()))
    print(100*"*")
    
    print("{} : Processing Log Data".format(datetime.now()))
    process_log_data(spark, input_data, output_data)
    print("{} : Log Data Processed".format(datetime.now()))
    print(100*"*")
if __name__ == "__main__":
    main()