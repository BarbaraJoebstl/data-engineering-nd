# Project: Data Lake with Spark

## About

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## Data Sets
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json


# TASK: 
create an etl.py that loads the data transform to tables according to requirements and loads the data back to s3

WIP
## Run jupyter notebook to develop locally in a docker container
We use a build based on the docker image "jupyter/pyspark-notebook".
Custom additions:
- libraries needed, as written in the requirements.txt
- jupyter config, so that we don't need a token for the notebook
- store your env variables in a `.env` file to access them
- the notebook is saved to the `data` folder

start container: `docker-compose up`, if you changed something in the config make sure to use `docker-compose up --force-recreate --build` to make sure changes are build

The user "jovian" stands for "a Jupiter-like planet" in the docker env.

 see ipynb: done: explore data and create tables

## TODO: 
- (get data from s3)
- store back to s3
- create functions for required etl.py from working notebook code.
- update readme


