import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.createOrReplaceTempView('songs')
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = song_df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])\
                           .withColumnRenamed('artist_name', 'name')\
                           .withColumnRenamed('artist_location', 'location')\
                           .withColumnRenamed('artist_latitude', 'lattitude')\
                           .withColumnRenamed('artist_longitude', 'longitude')\
                           .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.createOrReplaceTempView('artists')
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])\
                    .withColumnRenamed('userId', 'user_id')\
                    .withColumnRenamed('firstName', 'first_name')\
                    .withColumnRenamed('lastName', 'last_name')\
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table.createOrReplaceTempView('users')
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime')\
                          .withColumn('start_time', log_df.datetime)\
                          .withColumn("hour", hour("start_time"))\
                          .withColumn("day", dayofmonth("start_time"))\
                          .withColumn("week", weekofyear("start_time"))\
                          .withColumn("month", month("start_time"))\
                          .withColumn("year", year("start_time"))\
                          .withColumn("weekday", dayofweek("start_time"))
                          .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.createOrReplaceTempView('time')
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = log_df.join(song_df, col('artist') == col('artist_name'), 'inner')
    songplays_table = joined_df.select(['level', 'userId', 'ts', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent'])\
                               .withColumnRenamed('userId', 'user_id')\
                               .withColumnRenamed('ts', 'start_time')\
                               .withColumnRenamed('sessionId', 'session_id')\
                               .withColumnRenamed('sessionId', 'session_id')\
                               .withColumnRenamed('userAgent', 'user_agent')\
                               .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplay_table.createOrReplaceTempView('songplays')
    songplay_table.write.parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
