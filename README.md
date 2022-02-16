# Data Pipeline for Analyzing Goals Scored in Football Matches

## Overview
This project intends to create a set of data models for analyzing goals scored in football matches (soccer) for a group of scouts to gain insights of individual player and team performances. In particular, the group is interested in finding out the top players who are involved in a lot of goals (either scored or assisted). A data pipeline is constructed to extract data from a website called FootyStats (https://footystats.org/) through REST APIs, transform the data according to analytical needs, and load data into "Star-schemed" tables.

## Tools and Technologies
### Apache Airflow
Airflow is chosen as the workflow management tool for its simplicity, clarity, and flexibility in constructing and managing data pipelines.
The data pipeline created in this project needs to be run on a daily basis by 7 am every day. This can be easily set up and is taken care of by the scheduling functionality of Airflow.

### AWS Glue
The processing of data is written in PySpark and is run in an AWS Glue job. Glue is a fully-managed AWS service, meaning that there is no need to worry about the infrastructures. It also allows for easy scaling when data volume grows. For instance, if the data is increased by 100 times, we can still achieve the same level of performance by adjusting the number of workers accordingly in Glue job settings.

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


## Data Dictionary
### Fact Table
**goals**
| Column Name | Data Type  | Column Description |
| --------------- | --------------- | --------------- |
| goal_id | integer | Unique identifier of a goal. |
| match_id | integer | ID of the football match in which the goal was scored. |
| player_id | integer | ID of the player who scored the goal. |
| homeid | integer | ID of the team which played as the home team. |
| awayid | integer | ID of the team which played as the away team. |
| date_unix | integer | Match kickoff time in UNIX format. |
| assist_player_id | integer | ID of the player who assisted the goal. |
| extra | string | Additional information about the goal (an own goal or a penalty). |
| time | string | The minute in which the goal was scored. Allows '+' sign to indicate stoppage time. |

### Dimension Tables
**teams**
| Column Name | Data Type  | Column Description |
| --------------- | --------------- | --------------- |
| team_id | integer | Unique identifier of a football team. |
| name | string | Name of the team in native language. |
| english_name | string | Name of the team in English. |
| country | string | Country in which the team is based. |
| continent | string | Continent in which the team participates in competitions. |
| founded | integer | Year when the team was founded. |

**players**
| Column Name | Data Type  | Column Description |
| --------------- | --------------- | --------------- |
| Row 1 Column 1 | Row 1 Column 2 | Row 1 Column 3 |
| Row 2 Column 1 | Row 2 Column 2 | Row 2 Column 3 |
| Row 3 Column 1 | Row 3 Column 2 | Row 3 Column 3 |

**matches**
| Column Name | Data Type  | Column Description |
| --------------- | --------------- | --------------- |
| Row 1 Column 1 | Row 1 Column 2 | Row 1 Column 3 |
| Row 2 Column 1 | Row 2 Column 2 | Row 2 Column 3 |
| Row 3 Column 1 | Row 3 Column 2 | Row 3 Column 3 |

**time**
| Column Name | Data Type  | Column Description |
| --------------- | --------------- | --------------- |
| Row 1 Column 1 | Row 1 Column 2 | Row 1 Column 3 |
| Row 2 Column 1 | Row 2 Column 2 | Row 2 Column 3 |
| Row 3 Column 1 | Row 3 Column 2 | Row 3 Column 3 |


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


## How to Run the Pipeline
The following steps are required to run the pipeline.<br>
1. Create local staging directories on the server where Airflow is to run. For example, name the directories `/home/workspace/staging/summary/` and `/home/workspace/staging/detail/`. These are used as parameters in the `get_today_matches` task.
2. Create S3 bucket and folders. Pass them to the `upload_match_detail` task.
3. Create another S3 bucket which is used to hold Athena query results. Pass it to the `run_quality_checks` task.
4. Set up a Glue job in AWS and pass the job name to the `run_glue_job` task.
5. Start the Airflow server.
6. In the Airflow GUI interface, turn on the `footy_stats_dag`.


## Sample Queries
**List match details:**<br>

    select home_name, homegoalcount, awaygoalcount, away_name, stadium_name, ko_ts
    from "capstone"."matches" m, "capstone"."time" t
    where m.date_unix = t.date_unix
    limit 10

<img title="a title" alt="Alt text" src="/images/matches.png"></img>

**List the top 5 goal scorers:**<br>

    Select count(goal_id) as goals_scored, p.known_as as name
    from goals g, players p
    where g.player_id = p.player_id
    group by p.known_as
    order by goals_scored desc
    limit 5

<img title="a title" alt="Alt text" src="/images/top_goals.png"></img>
