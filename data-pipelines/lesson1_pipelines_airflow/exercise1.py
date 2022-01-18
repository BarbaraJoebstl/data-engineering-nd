# Instructions
# Define a function that uses the python logger to log a function. Then finish filling in the details of the DAG down below. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file or the video walkthrough on the next page.

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def my_function():
    logging.info('Hello World')


# every DAG needs a name and a start time
dag = DAG(
        'lesson1.exercise1',
        start_date=datetime.datetime.now())

# create a task 
# not calling the "my_function" from here, but passing it to the DAG, so that the executor knows what function to call
# add the dag so the pythonOperator knows what DAG to belong to.
greet_task = PythonOperator(
    task_id="task_exercise1",
    python_callable=my_function,
    dag=dag
)
