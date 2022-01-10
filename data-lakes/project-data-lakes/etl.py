from configparser import ConfigParser, NoSectionError, NoOptionError, ParsingError
from datetime import datetime
import os
from pyspark.sql import SparkSession, udf
from pyspark.sql import functions as F


def get_profile_credentials_for_udacity_sandbox():
    """
    Get's the credentials from a 'dl.cfg' file.
    You can use this when you are working in the udacity sandbox
    """
    config = ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def get_aws_credentials(user_name):
    """
    Returns aws credentials for a given profile name

    Params: 
        user_name: username, where we want to get the aws credentials from
    """

    config = ConfigParser()
    config.read([os.path.join(os.path.expanduser("~"), '.aws/credentials')])
    try:
        aws_access_key_id = config.get(user_name, 'aws_access_key_id')
        aws_secret_access_key = config.get(user_name, 'aws_secret_access_key')
    except ParsingError:
        print('Error parsing the config file for {}.'.format(user_name))
        raise
    except(NoSectionError, NoOptionError):
        try:
            aws_access_key_id = config.get('default', 'aws_access_key_id')
            aws_secret_access_key = config.get('default', 'aws_secret_access_key')
        except (NoSectionError, NoOptionError):
            print('Unable to find default AWS credentials')
            raise
    return aws_access_key_id, aws_secret_access_key


aws_access_key_id, aws_secret_access_key = get_aws_credentials('joebsbar')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Takes in song data from a path, creates tables for the given schema and writes them as parquet files to a given path

    Params:
        spark: the cursor object
        input_data: path to the data to process (in our case the song data)
        output_data: path to where the created parquet files will be written to
    """

    # get filepath to song data file
    # example: song_data/A/B/C/TRABCEI128F424C983.json, thats why we know the depth
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    print('---reading song data from s3')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'{output_data}/songs_table', mode='overwrite', partitiionBy=['year', 'artist_id'])
    print('---write songs_table to parquet file on {}'.format(output_data))


    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'title', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}/artists_table', mode='overwrite')
    print('---write artist_table to parquet file on {}'.format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Takes in log data from a path, creates tables for the given schema and writes them as parquet files to a given path

    Params:
        spark: the cursor object
        input_data: path to the data to process (in our case the song data)
        output_data: path to where the created parquet files will be written to
    """
    # get filepath to log data file
    log_data = f'{input_data}/log-data/*json'

    # read log data file
    df = spark.read.json(log_data)
    print('---reading song data from s3')

    # filter by actions for song plays
    df = df.filter(df['page']== 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.writeparquet(f'{output_data}/users_table', mode='overwrite')
    print('---write users_table to parquet file on {}'.format(output_data))

    # timetable with DF
    # 
    # time_table = df.withColumn('start_time', F.from_unixtime(F.col('ts')/1000))
    # time_table = time_table.select('ts', 'start_time') \
    #         .withColumn('year', F.year('start_time')) \
    #         .withColumn('month', F.month('start_time')) \
    #         .withColumn('week', F.weekofyear('start_time')) \
    #         .withColumn('weekday', F.dayofweek('start_time')) \
    #         .withColumn('day', F.dayofyear('start_time')) \
    #         .withColumn('hour', F.hour('start_time')) \

    # create start_time column from original timestamp column with a user defined function
    get_timestamp = udf(lambda timestamp: str(int(int(timestamp) / 1000)))
    # or spark.udf.register('get_timestamp', lambda timestamp: str(int(int(timestamp) / 1000))
    df = df.withColumn('start_time', get_timestamp(df['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda timestamp: str(datetime.fromtimestamp(int(timestamp) / 1000 )))
    df = df.withColumn('datetime', get_datetime(df['ts']))
    
    #variant SQL
    time_data = spark.sql("""
       SELECT DISTINCT 
           datetime as start_time,
           hour(datetime) AS hour,
           day(datetime) AS day,
           weekofyear(datetime) AS week,
           month(datetime) AS month,
           year(datetime) AS weekday
       FROM ts""")
    
    # write time table to parquet files partitioned by year and month
    time_data.write.parquet('f{output_data}/time_table', mode='overwrite', partitionBy=['year', 'month'])
    print('---write time_table to parquet file on {}'.format(output_data))

    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    song_data = spark.read.json(song_data)
    print('---reading song data')

    # create temporary views on the table -  in memory, no writing
    song_data.createOrReplaceTempView('song_data')
    log_data.createOrReplaceTempView('log_data')
    time_data.createOrReplaceTempView('time_data')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""SELECT DISTINCT
                                        l.ts as songplay_id,
                                        l.ts as start_time,
                                        l.userId as user_id,
                                        l.level as level,
                                        s.song_id as song_id,
                                        s.artist_id as artist_id,
                                        l.sessionId as session_id,
                                        l.location as location,
                                        l.userAgent as user_agent
                                    FROM song_data s
                                    JOIN log_data l
                                        ON s.artist_name = l.artist 
                                        AND s.title = l.song
                                        AND s.duration = s.lenght
                                    JOIN time_data t
                                        ON t.ts = l.ts
                                """).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'{output_data}/songplays_table', mode='overwrite', partition=['year', 'month'])
    print('---write songplays to parquet file on {}'.format(output_data))


def main():
    """
    create a spark session,
    load data from a given path
    process song and log data 
    writes new tables to a given output path

    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-barbara/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
