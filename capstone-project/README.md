Data Engineering Capstone Project

The project is developed in 5 steps:

Step 1: Scope the Project and Gather Data
Step 2: Explore and Assess the Data
Step 3: Define the Data Model
Step 4: Run ETL to Model the Data
Step 5: Complete Project Write Up

# Step1 - Scope the project and gather data:
This project will show how to load and transform data from four different data sources, load the data in spark apply quality checks and store the data into a star schema source-of-truth database so that it can be used for BI apps and ad hoc analysis.

As one of the acceptance criteria for this project is to handle at least 1 million rows and two different data sources and file formats, we will use the data sources are provided by Udacity. 

The main dataset includes data about _Immigration in the United States of America_ from [here](https://www.trade.gov/national-travel-and-tourism-office).
Supplementary datasets provided are:
- U.S. city _demographics_ from [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
- _Temperature_ data from [here](https://www.trade.gov/national-travel-and-tourism-office)
- Data on _airport codes_ from [here](https://datahub.io/core/airport-codes#data)


## Step 2 - Data exploration
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data. The data exploration can be found in `data/exploration.ipynb` notebook.

## Step3 - Explore the Data Model

Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model

As the data shall be used for BI apps and ad hoc analysis we want to use a star schema for the ease of use.

// Todo paste Star Schema Image
// Update types according to Spark


### Step 4: Run ETL to Model the Data
Create the data pipelines and the data model
- TODO: include a data dictionary




### Step 5: Complete Project write up
What's the goal? 
What queries will you want to run? How would Spark or Airflow be incorporated? 
Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.


### Additional questions:

Include a description of how you would approach the problem differently under the following scenarios:
#### 1. If the data was increased by 100x.


#### 2. If the pipelines were run on a daily basis by 7am.


#### 3. If the database needed to be accessed by 100+ people.




#TODO

- Move code from .ipynb to .py files
- Add .png of star schema
- Create dict for created star schema
- Answer questions