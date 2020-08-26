import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("CREDENTIALS",'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("CREDENTIALS",'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    Function that read and transform song_data files to

    save songs_table and artist_table on S3 (in parquet extension)

    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json" #real path
    # song_data = input_data + "song_data/A/B/C/TRABCEI128F424C983.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id", \
                            col("year").cast("int").alias("year"),\
                            col("duration").cast("float").alias("duration"))

    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', 'overwrite')


    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias("name"), \
                              col("artist_location").alias("location"), \
                              col("artist_latitude").cast("float").alias("lattitude"), \
                              col("artist_longitude").cast("float").alias("longitude"))

    artists_table = artists_table.dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', 'overwrite')


def process_log_data(spark, input_data, output_data):

    """
    Function that read and transform log_data files to

    save user_table, time_table and songplays_table on S3 (in parquet extension)

    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json" #real path
    # log_data = input_data + "log_data/2018/11/2018-11-12-events.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where("page='NextSong'")

    # extract columns for users table    
    user_table = df.select(col("userId").cast("int").alias("user_id"),\
                           col("firstName").alias("first_name"),\
                           col("lastName").alias("last_name"),"gender","level")
    
    user_table = user_table.dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(output_data + 'users/', 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x)/1000, IntegerType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime("timestamp"))
    
#     print(df.limit(5).toPandas().head())
    # extract columns to create time table
    time_table = df.select(col("timestamp").alias("start_time"),\
                           hour("datetime").alias("hour"),\
                           dayofmonth("datetime").alias("day"),\
                           weekofyear("datetime").alias("week"),\
                           month("datetime").alias("month"),\
                           year("datetime").alias("year"),\
                           date_format('datetime','E').alias('weekday')
                          )
#     print(time_table.limit(5).toPandas().head())
    time_table = time_table.dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/', 'overwrite')

    # read in song data to use for songplays table
#     song_df = spark.read.json(input_data + "song_data/A/B/C/TRABCEI128F424C983.json")
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias("a").join(song_df.alias("b"),\
                                         (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration)).\
    select(col("a.ts").alias("start_time"),col("a.userId").cast("int").alias("a.user_id"),"level",\
           col("a.sessionId").alias("session_id"),"a.location","a.userAgent","b.song_id","b.artist_id")

    
    get_start_time = udf(lambda x: datetime.fromtimestamp(int(x)/1000), TimestampType())
    songplays_table = songplays_table.withColumn("start_time", get_start_time("start_time"))
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = songplays_table.withColumn("year", year("start_time"))
    songplays_table = songplays_table.withColumn("month", month("start_time"))
    
#     print(songplays_table.limit(5).toPandas().head())

    songplays_table = songplays_table.dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/', 'overwrite')

def main():

    """
    Main function, first run process_song_data() which fill the data for songs_table and artist_table

    then, run process_log_data() which fill the data for user_table, time_table and songplays_table.
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-alindo/output/"
    
    process_song_data(spark, input_data, output_data) 
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
