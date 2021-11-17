shorteimport configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as TS

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = os.path.join(input_data, "song_data","*","*","*")
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int())
    ])
    print("Reading in song data...")
    df = spark.read.schema(songSchema)\
        .json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    print(songs_table.printSchema())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite")\
        .parquet(output_data + "songs.parquet")
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    print(artists_table.printSchema())
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
        .parquet(output_data + "artists.parquet")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    # Actually don't use this schema
    # logSchema = R([
        # Fld("artist", Str()),
        # Fld("auth", Str()),
        # Fld("firstName", Str()),
        # Fld("gender", Str()),
        # Fld("itemInSession", Int()),
        # Fld("lastName", Str()),
        # Fld("length", Dbl()),
        # Fld("level", Str()),
        # Fld("location", Str()),
        # Fld("method", Str()),
        # Fld("page", Str()),
        # Fld("registration", Int()),
        # Fld("sessionId", Int()),
        # Fld("song", Str()),
        # Fld("status", Int()),
        # Fld("ts", TS()),
        # Fld("userAgent", Str()),
        # Fld("userId", Str())
    # ])
    print("Reading in log data...")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter((df.page == 'NextSong') & (df.userId != ''))
    df = df.withColumn("itemInSession", col("itemInSession").cast(Int())) \
            .withColumn("registration", col("registration").cast(Int())) \
            .withColumn("sessionId", col("sessionId").cast(Int())) \
            .withColumn("status", col("status").cast(Int())) \
            .withColumn("ts", col("ts").cast(Dbl())) \
            .withColumn("userId", col("userId").cast(Int()))
    
    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")\
        .distinct()
    
    users_table.printSchema()
    
    # write users table to parquet files
    users_table.write.mode("overwrite")\
        .parquet(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)), Int())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda y: datetime.fromtimestamp((y/1000)), TS())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        col("timestamp").alias("start_time"),
        hour(col("datetime")).alias("hour"),
        dayofmonth(col("datetime")).alias("day"),
        weekofyear(col("datetime")).alias("week"),
        month(col("datetime")).alias("month"),
        year(col("datetime")).alias("year"),
        date_format(col("datetime"), "E").alias("weekday")
    ).distinct()
    
    time_table.printSchema()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite")\
        .parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    # is there a better way to do this by reading from spark or something?
    song_df = spark.read.parquet(output_data + "songs.parquet")
    artist_df = spark.read.parquet(output_data + "artists.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.alias("s")\
        .join(artist_df.alias("a"), artist_df.artist_id == song_df.artist_id)\
        .join(df.alias("l"), (df.artist == artist_df.artist_name) & (df.song == song_df.title))\
        .join(time_table.alias("t"), time_table.start_time == df.timestamp)\
        .select(monotonically_increasing_id().alias("songplay_id"),\
                col("l.timestamp").alias("start_time"),\
                col("t.year"),\
                col("t.month"),\
                col("l.userId").alias("user_id"),\
                col("l.level"),\
                col("s.song_id"),\
                col("a.artist_id"),\
                col("l.sessionId").alias("session_id"),\
                col("location"),\
                col("l.userAgent").alias("user_agent"))

    songplays_table.printSchema()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite")\
        .parquet(output_data + "songplay.parquet")


def main():
    spark = create_spark_session()
    #input_data = "data/"
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-oregon-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
