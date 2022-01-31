Data Engineering Capstone Project


This project will show how to build a datawarehouse to provide BI apps with that data.

As one of the acceptance criteria for this project is to handle at least 1 million rows and two different data sources and file formats, we will use the data sources are provided by Udacity. 

The main dataset includes data about _Immigration in the United States of America_ from [here](https://www.trade.gov/national-travel-and-tourism-office).
Supplementary datasets provided are:
- U.S. city _demographics_ from [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
- _Temperature_ data from [here](https://www.trade.gov/national-travel-and-tourism-office)
- Data on _airport codes_ from [here](https://datahub.io/core/airport-codes#data)

## Steps of the project
### Step 1: Scope the Project and Gather Data

We will use the provided datasets by udacity, to create a source-of-truth database that can be used for queries and as data source for BI apps.

### Step 2: Explore and Assess the Data
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data. The data exploration can be found in `data/exploration.ipynb` notebook.

#### Run locally 
For prototyping an testing it's nice to run Spark locally in a Docker container. Everything you need you'll find in the `local_setup` folder.
We use a build based on the docker image "jupyter/pyspark-notebook".
Custom additions:
- libraries needed, as written in the requirements.txt
- jupyter config, so that we don't need a token for the notebook
- store your env variables in a `.env` file to access them
- the notebook is saved to the `data` folder

start container: `docker-compose up`, if you changed something in the config make sure to use `docker-compose up --force-recreate --build` to make sure changes are build


### Step 3: Explore the Data Model 
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run ETL to Model the Data
Create the data pipelines and the data model
- TODO: include a data dictionary

### Step 5: Complete Project write up
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.


### Additional questions:

Include a description of how you would approach the problem differently under the following scenarios:
#### 1. If the data was increased by 100x.


#### 2. If the pipelines were run on a daily basis by 7am.


#### 3. If the database needed to be accessed by 100+ people.

