import re
from pyspark.sql.functions import udf
import pandas as pd


ports = {}


@udf
def sas_to_timestamp(date_sas: float) -> int:
    """
    transform and add new column
    https://online.stat.psu.edu/stat481/book/export/html/702
    https://stackoverflow.com/questions/36412864/convert-numeric-sas-date-to-datetime-in-pandas

    params: data_sas: a date in sas format
    return: a timestamp in seconds
    """

    if date_sas:
        datetime = pd.to_timedelta((date_sas), unit='D') + pd.Timestamp('1960-1-1')
        timestamp = datetime.timestamp()
        return timestamp

def get_ports():
    """
    creates a list of citys with its code
    data taken from SAS Description Labels "I94PORT"
    """ 

    with open("data/I94_SAS_Labels_Descriptions.SAS") as f:
        lines = f.readlines()

    re_compiled = re.compile(r"\'(.*)\'.*\'(.*)\'")
    for line in lines[302:961]:
        results = re_compiled.search(line)
        ports[results.group(1)] = results.group(2)


@udf
def map_city_name_to_port(city):

    for key in ports:
        if city.lower() in ports[key].lower():
            return key

def check_number_of_rows(df):
    """ counts the rows of a given dataframe

    :param df: spark dataframe to check counts on
    :param table_name: corresponding name of table
    """
    number_of_rows = df.count()
    if  number_of_rows == 0:
        print(f"Error: There are no records in {df}")
    else:
        print(f"Info: Current DF contains {number_of_rows} rows")

def check_number_of_columns(df, expected_colums):
    number_of_columns = len(df.columns)

    if number_of_columns != expected_colums:
        print(f"Error: There are {number_of_columns} instead of {expected_colums} for the current Dataframe")
    else:
        print(f"Info: {number_of_columns} columnns for current DF.")


