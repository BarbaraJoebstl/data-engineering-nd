#Instructions
#In this exercise, we’ll place our S3 to RedShift Copy operations into a SubDag.
#1 - Consolidate HasRowsOperator into the SubDag
#2 - Reorder the tasks to take advantage of the SubDag Operators

# Remember to run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.udacity_plugin import HasRowsOperator

from lesson3.exercise3.subdag import get_s3_to_redshift_dag
import sql_statements


start_date = datetime.datetime.utcnow()

dag = DAG(
    "lesson3.exercise3",
    start_date=start_date,
)

trips_task_id = "trips_subdag"
trips_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "lesson3.exercise3",
        trips_task_id,
        "redshift",
        "aws_credentials",
        "trips",
        sql_statements.CREATE_TRIPS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
        start_date=start_date,
    ),
    task_id=trips_task_id,
    dag=dag,
)

stations_task_id = "stations_subdag"
stations_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        "lesson3.exercise3",
        stations_task_id,
        "redshift",
        "aws_credentials",
        "stations",
        sql_statements.CREATE_STATIONS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
        start_date=start_date,
    ),
    task_id=stations_task_id,
    dag=dag,
)

#
# TODO: Consolidate check_trips and check_stations into a single check in the subdag
#       as we did with the create and copy in the demo
#
check_trips = HasRowsOperator(
    task_id="check_trips_data",
    dag=dag,
    redshift_conn_id="redshift",
    table="trips"
)

check_stations = HasRowsOperator(
    task_id="check_stations_data",
    dag=dag,
    redshift_conn_id="redshift",
    table="stations"
)

location_traffic_task = PostgresOperator(
    task_id="calculate_location_traffic",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.LOCATION_TRAFFIC_SQL
)

#
# TODO: Reorder the Graph once you have moved the checks
#
trips_subdag_task >> check_trips
stations_subdag_task >> check_stations
check_stations >> location_traffic_task
check_trips >> location_traffic_task
