from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3
import time


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks=[],
                 output_s3="",
                 region="",
                 aws_key="",
                 aws_secret="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.dq_checks = dq_checks
        self.output_s3 = output_s3
        self.region = region
        self.aws_key = aws_key
        self.aws_secret = aws_secret


    def execute(self, context):
        athena_client = boto3.client('athena', region_name=self.region, 
                           aws_access_key_id=self.aws_key,
                          aws_secret_access_key=self.aws_secret)
        
        for test in self.dq_checks:
            response = athena_client.start_query_execution(
                QueryString = test['check_sql'],
                QueryExecutionContext={
                    'Database': test['db']
                },
                ResultConfiguration={
                    'OutputLocation': self.output_s3
                }
            )
            
            # Athena is asynchronous.
            # May need loop a few times to get results back.
            while True:
                try:
                    query_results = athena_client.get_query_results(
                        QueryExecutionId=response['QueryExecutionId']
                    )
                    break
                except Exception as err:
                    print(err)
                    time.sleep(3)
            
            count = int(query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
            if count < test['expected_result']:
                raise ValueError(f"Data quality check failed. {test['check_sql']} contained 0 rows")
            else:
                self.log.info(f"Data quality check passed. {test['check_sql']} returned {count} records")
            
