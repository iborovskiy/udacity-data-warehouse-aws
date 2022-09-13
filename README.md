### Date created
2022-09-13


### Project Title
Udacity course project: Data Warehouse


### General Description
A startup called Sparkify wants to move their processes and data onto the cloud.

The goal of this project is to build an ETL pipeline that extracts source data from S3,
stages them in Redshift, and transforms data into a set of dimensional tables
for the analytics team to continue finding insights into what songs their users are listening to.

In this project, the following tasks were completed:
1. Establishing connection to pre-configured Redshift Cluster using IAM role that has read access to S3
2. Defining staging tables and analytics (fact & dimension) tables
3. Building ETL pipeline logic that loads data from S3 to staging tables on Redshift
4. Building ETL pipeline logic that loads data from staging tables to analytics tables


### Details of database schema design and ETL pipeline
For this project, we use a **star schema** optimized for queries on song play analysis.

Schema consists of the following tables:

_Fact tables (measurements, metrics or facts of a business process)_
**songplays** - records in log data associated with song plays

_Dimension tables (categorize facts and measures to answer a business questions)_
**users** - users in the app
**songs** - songs in music database
**artists** - artists in music database
**time** - timestamps of records in **songplays** broken down into specific units (hour, day, week, month, year, etc.)


**ETL pipeline** performs the following functions:
1. Loads data from S3 to staging tables on Redshift
2. Cleans and transforms this data
3. Loads processed data into analytics tables on Redshift


### Instructions for running the Python scripts
Run the following scripts in installed python interpreter:
- **create_tables.py** for creating database tables. You must run this script to reset your tables before each time you run ETL script.
- **etl.py** for executing ETL pipeline logic.

The following file is required for correct execution of the scripts above:
- **sql_queries.py** - contains all sql queries, and is imported into the files above.


For ETL to work correctly, you must meet the following prerequisites:
- Launch a redshift cluster and create an IAM role that has read access to S3
- Add redshift database (HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT) to **CLUSTER** section of **dwh.cfg**
- Add IAM role info (ARN) to **IAM_ROLE** section of **dwh.cfg**
- Add S3 links to song dataset (SONG_DATA) and log dataset (LOG_DATA, LOG_JSONPATH) to **S3** section of **dwh.cfg**


### Credits

**This entire project is based on learning materials from Udacity:**
https://learn.udacity.com/

The song dataset is a subset of real data from the Million Song Dataset:
http://millionsongdataset.com/

The log dataset consists of log files in JSON format generated by the following event simulator:
https://github.com/Interana/eventsim
