# -*- coding: utf-8 -*-
"""
Created on Mon Feb  7 16:37:05 2022

@author: u0032186
"""
import requests
import json
import pandas as pd
import time
from datetime import date
import boto3


#%% Static variables
api_key = 'example'
AWS_KEY=""
AWS_SECRET=""
region = 'us-east-1'


#%% League List
response = requests.get('https://api.football-data-api.com/league-list?key=example')
result = json.loads(response.text)
print(result)

# rows list initialization
rows = []

# appending rows
for data in result['data']:
    data_row = data['season']
    name = data['name']
    country = data['country']
    league_name = data['league_name']
    
    # put nested fields into separate rows  
    for row in data_row:
        row['name'] = name
        row['country']= country
        row['league_name'] = league_name
        rows.append(row)

# using data frame
df = pd.DataFrame(rows)
df

df.to_csv("league_list.csv", encoding='utf-8-sig')


#%% Country List
response = requests.get('https://api.football-data-api.com/country-list?key=example')
result = json.loads(response.text)
print(result)

with open('country_list.json', 'w', encoding='utf-8') as f:
    json.dump(result['data'], f, ensure_ascii=False)


#%% Matches by day

# Returns the current local date
today = date.today()
print("Today date is: ", today)

url = ('https://api.football-data-api.com/todays-matches?key={}&date={}').format(api_key, today)
response = requests.get(url)
result = json.loads(response.text)
print(result)


file_name = 'matches_{}.json'.format(today)
with open('matches_{}.json'.format(today), 'w', encoding='utf-8') as f:
    json.dump(result['data'], f, ensure_ascii=False)


# Upload local file to S3 bucket.
s3_client = boto3.client('s3', region_name=region,
                         aws_access_key_id=AWS_KEY,
                         aws_secret_access_key=AWS_SECRET)


bucket = 'footystats-capstone'
s3_client.upload_file(file_name, bucket, 'staging/' + file_name)


# Use match id from the daily file to retrieve match detail.
matches = result['data']

df = pd.DataFrame(matches)

for idx in df['id']:
    print(idx)


#%% Match Detail
for idx in df['id']:
    url = ("https://api.football-data-api.com/match?key=example&match_id={}").format(idx)
    response = requests.get(url)
    time.sleep(3)
    result = json.loads(response.text)

    path = ("matches/match_{}.json").format(idx)   
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(result['data'], f, ensure_ascii=False)


#%% Team
teams = pd.read_csv("team_id.csv")
print(teams)

for idx in teams['teamID']:
    print(idx)
    url = ("https://api.football-data-api.com/team?key=example&team_id={}").format(idx)
    response = requests.get(url)
    time.sleep(3)
    result = json.loads(response.text)
    
    path = ("teams/team_{}.json").format(idx)   
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(result['data'], f, ensure_ascii=False)


#%% Player
players = pd.read_csv("player_id.csv")
print(players)

for idx in players['player_id']:
    print(idx)
    url = ("https://api.football-data-api.com/player?key=example&player_id={}").format(idx)
    response = requests.get(url)
    time.sleep(3)
    result = json.loads(response.text)
    
    path = ("players/player_{}.json").format(idx)   
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(result['data'], f, ensure_ascii=False)


#%% League Season
response = requests.get('https://api.football-data-api.com/league-season?key=example&season_id=2012')
result = json.loads(response.text)
print(result)

with open('league_season_1.json', 'w', encoding='utf-8') as f:
    json.dump(result['data'], f, ensure_ascii=False)


#%% Retrieving All Premier League Matches from 2018/2019
response = requests.get('https://api.football-data-api.com/league-matches?key=example&league_id=1625')
#response = requests.get('https://api.footystats.org/league-matches?key=example&league_id=1625')

result = json.loads(response.text)
print(result)

matches = result['data']

for m in matches:
    print(m['home_name'] + ' vs ' + m['away_name'])

# Added filter to show only games that contained 3 goals or more.
for m in matches:
    if m['totalGoalCount'] >=3:
        print((m['home_name'] + ' {0}-' + '{1} ' + m['away_name']).\
              format(m['homeGoalCount'], m['awayGoalCount']))


# Retrieving All Registered Premier League Players from 2018/2019
response = requests.get('https://api.footystats.org/league-players?key=example&league_id=2012&page=1')
result = json.loads(response.text)
print(result)

players = result['data']


#%% Run AWS Glue ETL Job.
glue_client = boto3.client('glue', region_name=region, 
                           aws_access_key_id=AWS_KEY,
                          aws_secret_access_key=AWS_SECRET)

job_name = 'footy-stats-etl'
job_run_id = glue_client.start_job_run(JobName=job_name)

status = ""
while (status != 'SUCCEEDED' and status != 'FAILED'):
    time.sleep(10)
    status_detail = glue_client.get_job_run(JobName=job_name, RunId = job_run_id.get("JobRunId"))
    status = status_detail.get("JobRun").get("JobRunState")

if status == 'FAILED':
    raise Exception("Something went wrong! Check Glue job in AWS.")
else:
    print("Glue job succeeded!")


#%% Run data quality check.
athena_client = boto3.client('athena', region_name=region, 
                           aws_access_key_id=AWS_KEY,
                          aws_secret_access_key=AWS_SECRET)

query = """
SELECT count(*) FROM matches
"""

db = 'capstone'

output_s3 = 's3://udacity-athena-query-output'


response = athena_client.start_query_execution(
    QueryString = query,
    QueryExecutionContext={
        'Database': db
    },
    ResultConfiguration={
        'OutputLocation': output_s3
    }
)


while True:
    try:
        query_results = athena_client.get_query_results(
            QueryExecutionId=response['QueryExecutionId']
        )
        break
    except Exception as err:
        print(err)
        time.sleep(3)

print(json.dumps(query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'], indent=4, sort_keys=False))

print(query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])

count = int(query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
