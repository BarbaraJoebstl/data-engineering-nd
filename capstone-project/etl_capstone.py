from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, to_date
from pyspark.sql.types import StructType, StructField as Fld, StringType as Str, IntegerType as Int, ShortType as Short, DoubleType as Dbl, ByteType as Bt, TimestampType as Tst, FloatType as Flt
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from utils import check_number_of_columns, check_number_of_rows, sas_to_timestamp

OUTPUT_DATA_PATH = "tables"
INPUT_DATA_PATH = "data"

def create_spark_session():
    spark = SparkSession.builder().appName("Capstone Project").getOrCreate()
    return spark

def process_immigration_data(spark): 
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

    immigration_data = f'{INPUT_DATA_PATH}/sas_data' # holding the parquet files

    # instead of all 28 columns, we only need 10 for the tables we are going to create
    df_immigration = spark.read.parquet(immigration_data)\
                    .select('cicid', 'arrdate', 'depdate', 'i94cit', 'i94res', 'i94port', 'fltno', 'biryear', 'gender', 'i94visa')\
                    .schema(immigration_schema)

    df_immigration = df_immigration.dropDuplicates(['cicid'])

    # add timestamps, created with udf
    df_immigration = df_immigration.withColumn('arrival_ts', sas_to_timestamp(df_immigration['arrdate']))
    df_immigration = df_immigration.withColumn('departure_ts', sas_to_timestamp(df_immigration['depdate']))

    # create fact table
    fact_immigration = df_immigration.select('cicid', 'arrival_ts', 'departure_ts', 'i94cit', 'i94res', 'i94port', 'fltno').dropDuplicates()
    
    #df_immigration.printSchema()
    
    #test data
    check_number_of_rows(df_immigration)
    check_number_of_columns(df_immigration, 7)
    
    #write table
    fact_immigration.writeparquet(f'{OUTPUT_DATA_PATH}/fact_immigration', mode='overwrite')

    # TODO would be a possible second part in airflow, because dim tables can only be created once the fact table is in place
    create_dim_date(spark, df_immigration)
    create_dim_weather(spark)
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
    
    #test data
    check_number_of_rows(dim_time)
    check_number_of_columns(dim_time, 8)

    #write table
    dim_time.writeparquet(f'{OUTPUT_DATA}/dim_time', mode='overwrite', partitionBy=['year', 'month'])

def create_dim_city(spark):

    city_schema = StructType([
        Fld("city_name", Str()),
        Fld("state", Str()),
        Fld("median_age", Dbl()),
        Fld("male_population", Int()),
        Fld("total_population", Int()),
        Fld("foreign_born", Int()),
        Fld("average_householdsize", Dbl()),
        Fld("state_code", Int()),
    ])

    # todo pass in main, so that in can be easily changed
    fname = 'f{INPUT_DATA_PATH}/us-cities-demographics.csv'
    dim_city =  spark.read.option("delimiter", ";").csv(fname, schema=city_schema, header=True)

    #test data
    check_number_of_rows(dim_city)
    check_number_of_columns(dim_city, 8)
    
    #write table
    dim_city.writeparquet(f'{OUTPUT_DATA_PATH}/dim_city', mode='overwrite')

def create_dim_weather(spark):
    weather_schema = StructType([
        Fld("dt", DateType()),
        Fld("AverageTemperature", Flt()),
        Fld("AverageTemperatureUncertainty", Dbl()),
        Fld("City", Str()),
        Fld("Country", Str()),
        Fld("Latitude", Int()),
        Fld("Longitude", Int())
    ])

    fname = 'f{INPUT_DATA_PATH}/GlobalLandTemperaturesByCity.csv'

    df_temperature = spark.read.csv(fname, schema=weather_schema, header=True)

    # filter for us data
    df_temperature_us = df_temperature[df_temperature["Country"] == "United States"]
    # transform string type to datetype
    df_temperature_us = df_temperature_us.withColumn("date", to_date("dt"))
    # filter for date
    df_temperature_us = df_temperature_us.filter(df_temperature_us.date >= "1970-01-01")
    df_temperature_us = df_temperature_us.withColumn('year', year(df_temperature_us.date))
    df_temperature_us = df_temperature_us.withColumn('month', month(df_temperature_us.date))

    #test data
    check_number_of_rows(df_temperature_us)
    check_number_of_columns(df_temperature_us, 7)

    #write data
    df_temperature_us.write.parquet('./dim_temperature', mode='overwrite', partitionBy=['year', 'month'])
        
def create_dim_immigrant(df_immigration):
    dim_immigrant_person = df_immigration.select('cicid', 'biryear', 'gender', 'i94visa').dropDuplicates()
    
    #test data
    check_number_of_rows(dim_immigrant_person)
    check_number_of_columns(dim_immigrant_person, 4)
    
    #write data
    dim_immigrant_person.writeparquet('f{OUTPUT_DATA}/dim_immigrant_person', mode='overwrite')

def main():
    """
    create a spark session,
    load data from a given path
    process song and log data 
    writes new tables to a given output path

    """
    spark = create_spark_session()    
    process_immigration_data(spark)    


if __name__ == "__main__":
    main()
