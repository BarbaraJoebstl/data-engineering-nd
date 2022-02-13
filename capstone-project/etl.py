from configparser import ConfigParser, NoSectionError, NoOptionError, ParsingError
from datetime import datetime
import os
from pyspark.sql import SparkSession, udf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, Fieldtype as Fld, StringType as Str, IntegerType as Int, ShortType as Short, DoubleType as Dbl, ByteType as Bt
import pandas as pd

OUTPUT_DATA = "data-out"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

@udf
def sas_to_timestamp(date_sas: float) -> int:
    """
    User defined function to transform the sas timestamp into a timestamp
    params: data_sas: a date in sas format
    return: a timestamp in seconds
    """

    if date_sas:
        datetime = pd.to_timedelta((date_sas), unit='D') + pd.Timestamp('1960-1-1')
        timestamp = datetime.timestamp()
        return timestamp

def process_immigration_data(spark, input_data): 
    immigration_data = f'{input_data}/sas_data' # holding the parquet files

    # instead of all 28 columns, we only need 10 for the tables we are going to create
    immigration_schema = StructType([
        Fld("cicid", Short(), False),
        Fld("arrdate", Str()),
        Fld("depdate", Str()),
        Fld("i94cit", Short(), False),
        Fld("i94res", Short()),
        Fld("i94port", Str()),
        Fld("fltno", Str()),
        Fld("biryear", Short()),
        Fld("gender", Bt()),
        Fld("i94visa", Bt())
    ])

    df_immigration = spark.read.parquet(immigration_data)\
                    .select('cicid', 'arrdate', 'depdate', 'i94cit', 'i94res', 'i94port', 'fltno', 'biryear', 'gender', 'i94visa')\
                    .schema(immigration_schema)

    # add timestamps
    df_immigration = df_immigration.withColumn('arrival_ts', sas_to_timestamp(df_immigration['arrdate']))
    df_immigration = df_immigration.withColumn('departure_ts', sas_to_timestamp(df_immigration['depdate']))

    # store fact table
    fact_immigration = df_immigration.select('cicid', 'arrival_ts', 'departure_ts', 'i94cit', 'i94res', 'i94port', 'fltno').dropDuplicates()
    fact_immigration.writeparquet(f'{OUTPUT_DATA}/fact_immigration', mode='overwrite')

    # TODO would be a possible second part in airflow, because dim tables can only be created once the fact table is in place
    create_dim_date(spark, df_immigration)
    create_dim_city(spark, df_immigration)
    create_dim_immigrant(spark, df_immigration)

def create_dim_date(df_immigration):

    # get all existing timestamps and drop duplicats
    df_time = df_immigration.select('arrival_ts', 'departure_ts')
    df_time = df_time.select('arrival_ts').unionAll(df_time.select('departure_ts'))
    df_time = df_time.withColumnRenamed('arrival_ts', 'ts')
    df_time.dropDuplicates()

    # create dim_time table
    dim_time = df_time.select('ts') \
                .withColumn('date', F.from_unixtime(F.col('ts')/1000)) \
                .withColumn('year', F.year('ts')) \
                .withColumn('month', F.month('ts')) \
                .withColumn('week', F.weekofyear('ts')) \
                .withColumn('weekday', F.dayofweek('ts')) \
                .withColumn('day', F.dayofyear('ts')) \
                .withColumn('hour', F.hour('ts'))
    
    dim_time.writeparquet(f'{OUTPUT_DATA}/dim_time', mode='overwrite')


def create_dim_city(spark, df_immigration):
    # todo pass in main, so that in can be easily changed
    fname = 'data/us-cities-demographics.csv'
    city_schema = StructType([
        Fld("city_name", Str()),
        Fld("state", Str()),
        Fld("median_age", Dbl()),
        Fld("male_population", Int()),
        Fld("total_population", Int()),
        Fld("foreign_born", Int()),
        Fld("average_householdsize", Int()),
        Fld("state_code", Int()),
        Fld("race", Str())
    ])

    dim_city =  spark.read.option("delimiter", ";").csv(fname, schema=city_schema, header=True)
    city_code_df = df_immigration["i94cit"]
    dim_city_with_code = dim_city.join(city_code_df)

    dim_city_with_code.writeparquet(f'{OUTPUT_DATA}/dim_city', mode='overwrite')

    create_dim_weather(dim_city_with_code)

def create_dim_weather(dim_city):
    # load weather data, but only for us
    # compary by city name and store: cicd, city name, 
    # transform dt to month and year column
    # store avg temparature
    


def create_dim_immigrant(df_immigration):
    dim_immigrant_person = df_immigration.select('cicid', 'biryear', 'gender', 'i94visa').dropDuplicates()
    dim_immigrant_person.writeparquet('f{OUTPUT_DATA}/dim_immigrant_person', mode='overwrite')


def main():
    """
    create a spark session,
    load data from a given path
    process song and log data 
    writes new tables to a given output path

    """
    spark = create_spark_session()
    input_data = "data"
    output_data = "data-out"
    
    process_immigration_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()
