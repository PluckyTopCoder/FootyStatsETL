from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3
import time


class RunGlueJobOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 region="",
                 aws_key="",
                 aws_secret="",
                 glue_job="",
                 *args, **kwargs):

        super(RunGlueJobOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.region = region
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.glue_job = glue_job        

    def execute(self, context):
        glue_client = boto3.client('glue', region_name=self.region, 
                           aws_access_key_id=self.aws_key,
                          aws_secret_access_key=self.aws_secret)


        job_run_id = glue_client.start_job_run(JobName=self.glue_job)
        status = ""
        # Use a loop to check Glue job run status
        while (status != 'SUCCEEDED' and status != 'FAILED'):
            time.sleep(10)
            status_detail = glue_client.get_job_run(JobName=self.glue_job, RunId = job_run_id.get("JobRunId"))
            status = status_detail.get("JobRun").get("JobRunState")

        if status == 'FAILED':
            raise Exception("Something went wrong! Check Glue job in AWS.")
        else:
            self.log.info("Glue job succeeded!")
        
