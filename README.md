# Data Pipeline for Analyzing Goals Scored in Football Matches

## Overview
This project intends to create a set of data models for analyzing goals scored in football matches (soccer) for a group of scouts to gain insights of individual player and team performances. In particular, the group is interested in finding out the top players who are involved in a lot of goals (either scored or assisted). A data pipeline is constructed to extract data from a website called FootyStats (https://footystats.org/) through REST APIs, transform the data according to analytical needs, and load data into "Star-schemed" tables.

## Tools and Technologies
### Apache Airflow
Airflow is chosen as the workflow management tool for its simplicity, clarity, and flexibility in constructing and managing data pipelines.

### AWS Glue
The processing of data is written in PySpark and is run in an AWS Glue job. Glue is a fully-managed AWS service, meaning that there is no need to worry about the infrastructures. It also allows for easy scaling when data volume grows.

### AWS S3
The raw data and processed data are stored in AWS S3, which offers industry-leading scalability, data availability, security, and performance.

### AWS Athena
Athena is an interactive query service that makes it easy to analyze data stored in S3 using standard SQL. Even when 100+ users query the tables at the same time, there will always be enough computing resources to get fast, interactive query performance.


## Pipeline Design
<img title="a title" alt="Alt text" src="/images/airflow-pipeline.png"></img>
As shown in the above screen shot, the workflow includes the following steps: <br>
1. **Begin_execution** - an instance of `DummyOperator` that indicates the start of workflow.
2. **get_today_matches** - custom operator to retrieve a summary of the matches played today and the detail of each match through REST API. The responses are saved in JSON format to local staging directories.
3. **upload_match_detail** - custom operator to upload the staging files to an S3 bucket so that a Glue job can process them.
4. **run_glue_job** - custom operator to start the Glue job. It checkes periodically for the job completion.
5. **Run_data_quality_checks** - custom operator to verify tables have been properly loaded.
6. **Stop_execution** - an instance of `DummyOperator` that indicates the end of workflow.


## Database Design
The database tables employ a Star Schema with the purpose to optimize queries on goal analysis.
<img title="a title" alt="Alt text" src="/images/db_design.png"></img>


## Files in the Repository

### Datasets
Datasets are located in the `data` folder. There are sample data for `teams`, `players`, and `matches`. These have been retrieved through REST API calls to the FootyStats (https://footystats.org/) website, and can be used for data exploration.

  
### API test scripts
The `api` folder contains a python script to help you understand how to utilize the set of REST APIs to retrieve data from FootyStats website.
**Please note** that to fully access the APIs you need a **paid subscription**. However, you will be able to use the `example` key for testing purpose, although in that case the scope of the matches is limited to English Premier League only with an hourly quota of 180 API requests.


### PySpark script
There is a PySpark script `footy-stats-etl.py` in the `aws` folder. The script is to be placed in an AWS Glue job and triggered by Airflow DAG.


### Airflow DAGs
The `airflow` folder contains code for Airflow DAG and custom operators.


## How to Run the Scripts
In Jupyter Lab, start a new command line interface by clicking "File" -> "New" -> "Terminal".<br>
1. Run command `python create_tables.py`.
2. Run command `python etl.py`.<br>

Remember to rerun `create_tables.py` to reset your tables before each time you run `etl.py`.


