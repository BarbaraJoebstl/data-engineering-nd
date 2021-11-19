# Project 1: Sparkify Data Pipeline

## About 
This project is the part the section "Data Modeling with Postgress". In this project the skills learned in this class will be applied.

The online class covered the following topics:
 - What makes a database a relational database and *Codd’s 12 rules of relational database design*
 - The difference between different types of workloads for *databases OLAP and OLTP*
 - The process of database *normalization* and the normal forms.
 - *Denormalization* and when it should be used.
 - *Fact vs dimension tables* as a concept and how to apply that to our data modeling
 - How the *star and snowflake schemas* use the concepts of fact and dimension tables to make getting value out of the data easier.

We have data about songs and user activity for the music streaming app Sparkify.
Based on two datasets
 - song dataset
 - log dataset 
 we create a database schema and ETL pipeline for an easier and better analysis of the data.
 
## Schema
We create a star schema to optimize queries for our analysis.

### Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong

### Dimension Tables
*users*: - users in the app
*songs*: - songs in music database
*artists*: - artists in music database
*time*: - timestamps of records in songplays broken down into specific units

![Image caption](https://drive.google.com/file/d/1qh-xQM4LYTLRriHnjiOztjL2x_nuqQQL/view?usp=sharing)

### ETL (Extract, Transform, Load) Process
#### Create Tables
`create_tables.py` is used to establish the database connection and create or delete tables.
`sql_queries.py` is used to create the above mentioned tables.
`test.ipynb` is used to test and see if tables are created properly and data can be stored.Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

#### Process Data
The `etl.ipynb` is used to test the sequences. 
First we load data from _song_data_ and extract the values for the *song table* and *artist table*. 
Then we load data from _log_data_ and extract values for the *user table* and the *time table*.
In the last step we combine the extracted data into *songplays table*.

The code used in `etl.py` is based on the notebook code.

Once the whole pipeline works, everything can be run with:

`!python create_tables.py` - delete previous and create new tables
`!python etl.py` - !python create_tables.py

from the console.

To make sure everything was inserted correctly run `test.ipynb` again.

### Furter Information
[Cheatsheet and more info links](https://video.udacity-data.com/topher/2019/October/5d9683fc_dend-p1-lessons-cheat-sheet/dend-p1-lessons-cheat-sheet.pdf)

### TODO develop locally with docker