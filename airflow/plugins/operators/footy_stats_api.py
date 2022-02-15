from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import json
import time
import pandas as pd
import os
from glob import glob


class FootyStatsAPIOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 api_key="",
                 summary_path="",
                 detail_path="",
                 *args, **kwargs):

        super(FootyStatsAPIOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.api_key = api_key
        self.summary_path = summary_path
        self.detail_path = detail_path
        

    def execute(self, context):
        # clear local staging folders
        files = glob(self.summary_path + f"*.json")
        for f in files:
            os.remove(f)
        
        files = glob(self.detail_path + f"*.json")
        for f in files:
            os.remove(f)
        
        # Get a list of matches played on a particular day
        self.date = context["execution_date"].strftime("%Y-%m-%d")        
        url = ('https://api.football-data-api.com/todays-matches?key={}&date={}').format(self.api_key, self.date)
        self.log.info(url)
        response = requests.get(url)
        time.sleep(3)
        result = json.loads(response.text)
        
        file_name = self.summary_path + 'matches_{}.json'.format(self.date)
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(result['data'], f, ensure_ascii=False)
        
        self.log.info(result['pager'])
        
        # Get detail of each match
        df = pd.DataFrame(result['data'])
        for idx in df['id']:
            url = ("https://api.football-data-api.com/match?key={}&match_id={}").format(self.api_key, idx)
            self.log.info(url)
            response = requests.get(url)
            time.sleep(3)
            result = json.loads(response.text)
        
            file_name = self.detail_path + 'match_{}.json'.format(idx)
            with open(file_name, 'w', encoding='utf-8') as f:
                json.dump(result['data'], f, ensure_ascii=False)

            self.log.info(result['pager'])

