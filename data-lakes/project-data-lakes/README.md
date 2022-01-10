# Project: Data Lake with Spark

## About

This is the assignment for the part *Data Lakes with Spark* of the Udacity Nanodegree progamm. 
The aim of this part is to learn:
- Why we nee Data Lakes
- Big data technology effect on data warehousing
- How to do Data Lakes
- Big Data on AWS_The Options
- Data Lake Issues

#### Data introduction - assignment
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


##### Data Sets
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

### Assignment
The task is to build an ETL pipeline for a data lake hosted on S3. We need to load data from S3, process the data into the analytics tables (see image below) using Spark, and load them back into S3. This Spark process will be deployed on a cluster using AWS. 


## Prerequisites
### Run locally 
For prototyping an testing it's nice to run Spark locally in a Docker container. Everything you need you'll find in the `local-setup` folder.
We use a build based on the docker image "jupyter/pyspark-notebook".
Custom additions:
- libraries needed, as written in the requirements.txt
- jupyter config, so that we don't need a token for the notebook
- store your env variables in a `.env` file to access them
- the notebook is saved to the `data` folder

start container: `docker-compose up`, if you changed something in the config make sure to use `docker-compose up --force-recreate --build` to make sure changes are build

### AWS
- AWS CLI installed [Help](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- Set up Access credentials using AWS IAM. Create a User with admin rights and programatic access. Check with `aws iam list-users`
- Create an EMR Cluster
  -- `aws emr create-default-roles` to create an EMR_EC2_DefaultRole for your account
  -- launch a cluter `emr create-cluster --name {your-name} --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark --ec2-attributes KeyName={yourKey} --instance-type m5.xlarge --instance-count 3`
  Make sure you have the .pem for your EC2 in place. Don't forget to terminate the cluster once you are done or add the `--auto-terminate` flag, when creating the cluster
- Create an S3 Bucket for the output data. Make sure the permissions are set correctly
- Connect to cluster via SSH
  -- Edit the security groups inbound rules to authorize SSH traffic (port 22) from your computer [Help](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-ssh-prereqs.html)
  -- Verify connection to the Master node: EC2 -> Instances -> your master node -> Connect to instance [Help](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html)

## ETL Pipeline
### A: Data Exploration and testing 
See `local_setup\data\assignment_data_lakes.ipynb`. You cann see the data schema, how to transform the data to tables and how to write them to parquet files.

#### Load Data
- Load the data from S3 with spark.read.json() and have a look at the schemas.
![schema_initial](https://user-images.githubusercontent.com/6280553/148812051-71646a04-6edb-43ba-ac2d-73acaa14d8e7.png)


#### Table schemas for analysis
![schema](https://user-images.githubusercontent.com/6280553/148812186-9088e881-19c9-429e-bdd3-4310652b3b0d.png)



### B: `etl.py`

In the etl.py you can find the code from the notebook, refactored as a script. To run the script use `python etl.py`

## Additional Info

- [Docstrings](https://www.python.org/dev/peps/pep-0257/)
- [Why PEP8](https://realpython.com/python-pep8/)
- [PEP 8 Styleguide](http://pep8online.com/)

### EMR - Elastic Map Reduce

Since a Spark cluster includes multiple machines, in order to use Spark code on each machine, we would need to download and install Spark and its dependencies. This is a manual process. Elastic Map Reduce is a service offered by AWS that negates the need for you, the user, to go through the manual process of installing Spark and its dependencies for each machine.
`emr create-cluster --name {your-name} --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark --ec2-attributes KeyName={yourKey} --instance-type m5.xlarge --instance-count 3 `

Make sure you have the .pem for your EC2 in place.

Make sure to terminate the cluster once you are done or add the `--auto-terminate` flag, when creating the cluster

### Differnces S3 (Simple Storage Service) and HDFS (Hadoop Distributed File System)

AWS S3 is an object storage system that stores the data using key value pairs, namely bucket and key, and HDFS is an actual distributed file system which guarantees fault tolerance. HDFS achieves fault tolerance by having duplicate factors, which means it will duplicate the same files at 3 different nodes across the cluster by default (it can be configured to different numbers of duplication).
HDFS has usually been installed in on-premise systems, and traditionally have had engineers on-site to maintain and troubleshoot Hadoop Ecosystem, which cost more than having data on cloud. Due to the flexibility of location and reduced cost of maintenance, cloud solutions have been more popular. With extensive services you can use within AWS, S3 has been a more popular choice than HDFS.
Since AWS S3 is a binary object store, it can store all kinds of format, even images and videos. HDFS will strictly require a certain file format - the popular choices are avro and parquet, which have relatively high compression rate and which makes it useful to store large dataset.

### Parquet

https://www.upsolver.com/blog/apache-parquet-why-use


