import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType

'''
Creating a connection to AWS with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
'''

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

'''
Creating a spark session.
'''    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark



def process_song_data(spark, input_data, output_data):
    
'''
Function to process song data from the "input_data" path and export ".parquet" files to the "output_data" path.
Input_data and output_data are defined in the "main" function below.
Song_df is created from data residing on AWS S3 in "input_data" location.
"Songs_table" and "artists_table" are created based on song_df data.
'''
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    song_df = spark.read.json(song_data)


    # extract columns to create songs table
    songs_table = song_df.select('song_id', 'title', 'artist_id','year', 'duration')\
    .withColumnRenamed('title', 'song_title')\
    .withColumnRenamed('year', 'song_year')\
    .withColumnRenamed('duration', 'song_duration')\
    .dropDuplicates(['song_id'])
    
    songs_table.createOrReplaceTempView('songs')

    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('song_year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')


    # extract columns to create artists table
    artists_table = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates(['artist_id'])
    artists_table.createOrReplaceTempView('artists')

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')

    


def process_log_data(spark, input_data, output_data):
    
'''
Function to process log data from the "input_data" path and export ".parquet" files to the "output_data" path.
Input_data and output_data are defined in the "main" function below.
log_df is created from data residing on AWS S3 in "input_data" location.
"filter_table", "users_table" and "time_table" are created based on log_df data.
"songplays_table" is created based on "song_df" and "log_df" data.
'''

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file

    log_df = spark.read.json(log_data)

    
    # filter by actions for song plays
    filter_table = log_df.filter(log_df.page =='NextSong').select('ts', 'userId', 'level', 'song', 'artist','sessionId', 'location', 'userAgent')
    
    
    # extract columns for users table    
    users_table = log_df.select('userId', 'firstName', 'lastName','gender', 'level')\
    .withColumnRenamed('firstName', 'user_firstname')\
    .withColumnRenamed('lastName', 'user_lastname')\
    .withColumnRenamed('gender', 'user_gender')\
    .withColumnRenamed('level', 'user_level')\
    .dropDuplicates(['userId'])
    
    users_table.createOrReplaceTempView('users')

    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn('timestamp', get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    log_df = log_df.withColumn('start_time', get_timestamp(col('ts')))
  
    
    # extract columns to create time table
    log_df = log_df.withColumn('ts_hour', hour('timestamp'))
    log_df = log_df.withColumn('ts_day', dayofmonth('timestamp'))
    log_df = log_df.withColumn('ts_month', month('timestamp'))
    log_df = log_df.withColumn('ts_year', year('timestamp'))
    log_df = log_df.withColumn('ts_week', weekofyear('timestamp'))
    log_df = log_df.withColumn('ts_weekday', dayofweek('timestamp'))
    
    
    time_table = log_df.select(
        col('start_time'), 
        col('ts_hour'), 
        col('ts_day'), 
        col('ts_week'),
        col('ts_month'),
        col('ts_year'), 
        col('ts_weekday')
    ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('ts_year', 'ts_month').parquet(os.path.join(output_data,'time/time.parquet'), 'overwrite')
    

     # extract columns from joined song and log datasets to create songplays table 

    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_df = spark.read.json(song_data)

    
    songplays_table = log_df.join(song_df, song_df.title==log_df.song, 'inner')\
    .select(col('start_time'),
            col('userId').alias('user_id'),
            col('level').alias('user_level'),
            col('song_id'),
            col('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('user_location'), 
            col('userAgent').alias('user_agent'),
            col('ts_year'),
            col('ts_month'))\
    .withColumn('songplay_id', monotonically_increasing_id())
                                 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('ts_year', 'ts_month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sa-dend/"
    
    process_song_data(spark, input_data, output_data)   
    print("Songs DF processed")
    process_log_data(spark, input_data, output_data)
    print("Logs DF processed")


if __name__ == "__main__":
    main()
