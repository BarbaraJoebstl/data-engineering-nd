#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple PythonOperators
#3 - Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator



def log_youngest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear
        FROM younger_riders
        ORDER BY birthyear DESC LIMIT 1""")
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

def load_and_analyze(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")

    # Find all trips where the rider was under 18
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """)
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")


    # Find out how often each bike is ridden
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid)
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """)

    # Count the number of stations by city
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """)


def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

load_and_analyze = PythonOperator(
    task_id='load_and_analyze',
    dag=dag,
    python_callable=load_and_analyze,
    provide_context=True,
)

create_oldest_task = PostgresOperator(
    dag=dag,    
    task_id="create_oldest",
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)

create_youngest_task = PostgresOperator(
    dag=dag,    
    task_id="create_youngest",
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_youngest_task = PythonOperator(
    task_id="log_youngest",
    dag=dag,
    python_callable=log_youngest
)

lifetime_rides_task = PostgresOperator(
    task_id="lifetime_rides",
    dag=dag,
    sql="""
    BEGIN;
    DROP TABLE IF EXISTS lifetime_rides:
    CREATE TABLE lifetime_rides AS(
        SELECT bikeid, COUNT(bikeid)
        FROM trips
        GROUP BY bikeid
    );
    COMMIT;
    """,
    postgress_conn_id="redshift"
)

city_station_count_task = PostgresOperator(
    task_id="city_station_count",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXIST city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)
            

get_usage_by_bike_id = PostgresOperator(
    task_id = 'usage_by_bike_id',
    dag=dag,
    sql="""
    BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid)
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

usage_by_bike_id = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)


create_oldest_task >> log_oldest_task
create_youngest_task << log_youngest_task

