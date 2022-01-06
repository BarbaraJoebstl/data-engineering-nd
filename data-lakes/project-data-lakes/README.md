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

WIP
## Run jupyter notebook to develop locally in a docker container
- use "jupyter/pyspark-notebook" image
- go to compose/docker-compose.yaml and run `docker-compose up` to start the notebook, if you re changing something in the docker file make sure to use `docker-compose up --force-recreate --build` so that the changes will be done

- see this tutorial to also use use s3 or simply run `!import boto3` inside the running docker container to downlaod the data. If you don't store it you will need to rerun it every time to fetch the data.

## Next step: 
- get data from s3
- explore data
- export fact and dimension tables
- store back to s3


