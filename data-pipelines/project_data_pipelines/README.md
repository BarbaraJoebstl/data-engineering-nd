# Assignment: Data Pipelines with Airflow

## About

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to create high grade data pipelines that

- are dynamic
- built from reusable tasks
- can be monitored
- allow easy backfills
- run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift.

The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Datasets

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

## Overview
This assignment displays core concepts of Apache Airflow.
create your own custom operators to perform tasks:

- staging the data,
- filling the data warehouse,
- and running checks on the data as the final step.

The project template package contains three major components for the project:

- The dag template has all the imports and task templates in place, but the task dependencies have not been set
- The operators folder with operator templates
- A helper class for the SQL transformations including a helpers class for the SQL transformations.

## Prerequisites
1. Create a Redshift Cluster if you like [progamatically](https://github.com/BarbaraJoebstl/data-engineering-nd/blob/master/data-warehouse/lesson3-cloud-computing/IaC_Redshift.ipynb)
2. Install Apache Airflow and Boto (you can run `pip install requirements.txt`)
3. When Airflow is running add connections in the Airflow Web UI (Admin -> Connections):
   Conn Id: `aws_credentials`.
   Conn Type: `Amazon Web Services`.
   Login: <Access key ID> from the IAM User credentials.
   Password: <Secret access key> from the IAM User credentials.
4. Create Connection page:
   Conn Id: `redshift`.
   Conn Type: `Postgres`.
   Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console (without portnumber)
   Schema: `dev` This is the Redshift database you want to connect to.
   Login: `awsuser`
   Password: the password you created when launching your Redshift cluster.
   Port: `5439`.
   !: Delete the cluster eacht time you are finiseh working.

## Building the operators
In order to complete the project, there are four operators to be implemented

### Stage Operator
- Load JSON fromatted files from S3 to Amazon Redshift (SQL Copy)
- contains timestamp
- runs backfills

### Fact and Dimension Operator
- uses the provided SQL helper class to run the data transformations

### Data Quality Operator
- runs checks on the data

